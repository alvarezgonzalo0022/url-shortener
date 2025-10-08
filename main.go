package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	charset    = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	shortIDLen = 7
	cacheTTL   = 24 * time.Hour
)

type URLShortener struct {
	redis      *redis.Client
	mongo      *mongo.Collection
	statsQueue chan StatsEvent
	baseURL    string
}

type URL struct {
	ID        string    `bson:"_id" json:"id"`
	LongURL   string    `bson:"long_url" json:"long_url"`
	Clicks    int64     `bson:"clicks" json:"clicks"`
	CreatedAt time.Time `bson:"created_at" json:"created_at"`
}

type StatsEvent struct {
	ShortID   string
	Timestamp time.Time
}

type CreateURLRequest struct {
	LongURL string `json:"long_url"`
}

type CreateURLResponse struct {
	ShortURL string `json:"short_url"`
	ShortID  string `json:"short_id"`
}

type StatsResponse struct {
	ShortID   string    `json:"short_id"`
	LongURL   string    `json:"long_url"`
	Clicks    int64     `json:"clicks"`
	CreatedAt time.Time `json:"created_at"`
}

func NewURLShortener() (*URLShortener, error) {
	redisURL := getEnv("REDIS_URL", "localhost:6379")
	mongoURL := getEnv("MONGO_URL", "mongodb://localhost:27017")
	mongoDB := getEnv("MONGO_DB", "urlshortener")
	baseURL := getEnv("BASE_URL", "http://localhost:8080")

	rdb := redis.NewClient(&redis.Options{
		Addr:         redisURL,
		PoolSize:     100,
		MinIdleConns: 20,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	mongoClient, err := mongo.Connect(ctx, options.Client().
		ApplyURI(mongoURL).
		SetMaxPoolSize(50).
		SetMinPoolSize(10))
	if err != nil {
		return nil, fmt.Errorf("mongo connection failed: %w", err)
	}

	if err := mongoClient.Ping(ctx, nil); err != nil {
		return nil, fmt.Errorf("mongo ping failed: %w", err)
	}

	collection := mongoClient.Database(mongoDB).Collection("urls")

	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "created_at", Value: -1}},
	})
	if err != nil {
		log.Printf("Warning: failed to create index: %v", err)
	}

	us := &URLShortener{
		redis:      rdb,
		mongo:      collection,
		statsQueue: make(chan StatsEvent, 10000),
		baseURL:    baseURL,
	}

	go us.statsAggregator()

	return us, nil
}

func (us *URLShortener) generateShortID() string {
	b := make([]byte, shortIDLen)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func (us *URLShortener) createURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CreateURLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.LongURL == "" {
		http.Error(w, "long_url is required", http.StatusBadRequest)
		return
	}

	if !strings.HasPrefix(req.LongURL, "http://") && !strings.HasPrefix(req.LongURL, "https://") {
		http.Error(w, "URL must start with http:// or https://", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	var shortID string
	for i := 0; i < 5; i++ {
		shortID = us.generateShortID()
		
		exists, _ := us.redis.Exists(ctx, shortID).Result()
		if exists == 0 {
			count, err := us.mongo.CountDocuments(ctx, bson.M{"_id": shortID})
			if err == nil && count == 0 {
				break
			}
		}
		
		if i == 4 {
			http.Error(w, "Failed to generate unique ID", http.StatusInternalServerError)
			return
		}
	}

	urlDoc := URL{
		ID:        shortID,
		LongURL:   req.LongURL,
		Clicks:    0,
		CreatedAt: time.Now(),
	}

	_, err := us.mongo.InsertOne(ctx, urlDoc)
	if err != nil {
		log.Printf("MongoDB insert error: %v", err)
		http.Error(w, "Failed to create short URL", http.StatusInternalServerError)
		return
	}

	if err := us.redis.Set(ctx, shortID, req.LongURL, cacheTTL).Err(); err != nil {
		log.Printf("Redis cache error: %v", err)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(CreateURLResponse{
		ShortURL: fmt.Sprintf("%s/%s", us.baseURL, shortID),
		ShortID:  shortID,
	})
}

func (us *URLShortener) redirect(w http.ResponseWriter, r *http.Request) {
	shortID := strings.TrimPrefix(r.URL.Path, "/")
	
	if shortID == "" || shortID == "api" || strings.HasPrefix(shortID, "api/") {
		http.NotFound(w, r)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	longURL, err := us.redis.Get(ctx, shortID).Result()
	if err == nil {
		select {
		case us.statsQueue <- StatsEvent{ShortID: shortID, Timestamp: time.Now()}:
		default:
		}
		
		http.Redirect(w, r, longURL, http.StatusMovedPermanently)
		return
	}

	var urlDoc URL
	err = us.mongo.FindOne(ctx, bson.M{"_id": shortID}).Decode(&urlDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			http.NotFound(w, r)
		} else {
			log.Printf("MongoDB query error: %v", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	go func() {
		ctx := context.Background()
		us.redis.Set(ctx, shortID, urlDoc.LongURL, cacheTTL)
	}()

	select {
	case us.statsQueue <- StatsEvent{ShortID: shortID, Timestamp: time.Now()}:
	default:
	}

	http.Redirect(w, r, urlDoc.LongURL, http.StatusMovedPermanently)
}

func (us *URLShortener) getStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	shortID := strings.TrimPrefix(r.URL.Path, "/api/stats/")
	if shortID == "" {
		http.Error(w, "Short ID required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var urlDoc URL
	err := us.mongo.FindOne(ctx, bson.M{"_id": shortID}).Decode(&urlDoc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			http.NotFound(w, r)
		} else {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(StatsResponse{
		ShortID:   urlDoc.ID,
		LongURL:   urlDoc.LongURL,
		Clicks:    urlDoc.Clicks,
		CreatedAt: urlDoc.CreatedAt,
	})
}

func (us *URLShortener) deleteURL(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	shortID := strings.TrimPrefix(r.URL.Path, "/api/urls/")
	if shortID == "" {
		http.Error(w, "Short ID required", http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	result, err := us.mongo.DeleteOne(ctx, bson.M{"_id": shortID})
	if err != nil {
		log.Printf("MongoDB delete error: %v", err)
		http.Error(w, "Failed to delete URL", http.StatusInternalServerError)
		return
	}

	if result.DeletedCount == 0 {
		http.NotFound(w, r)
		return
	}

	us.redis.Del(ctx, shortID)

	w.WriteHeader(http.StatusNoContent)
}

func (us *URLShortener) statsAggregator() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	batch := make(map[string]int64)

	for {
		select {
		case event := <-us.statsQueue:
			batch[event.ShortID]++

		case <-ticker.C:
			if len(batch) == 0 {
				continue
			}

			currentBatch := make(map[string]int64, len(batch))
			for k, v := range batch {
				currentBatch[k] = v
			}
			batch = make(map[string]int64)

			go us.updateClickStats(currentBatch)
		}
	}
}

func (us *URLShortener) updateClickStats(batch map[string]int64) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for shortID, clicks := range batch {
		_, err := us.mongo.UpdateOne(
			ctx,
			bson.M{"_id": shortID},
			bson.M{"$inc": bson.M{"clicks": clicks}},
		)
		if err != nil {
			log.Printf("Failed to update stats for %s: %v", shortID, err)
		}
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func main() {
	rand.Seed(time.Now().UnixNano())

	shortener, err := NewURLShortener()
	if err != nil {
		log.Fatalf("Failed to initialize URL shortener: %v", err)					
	}

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("healthy\n"))
	})
	http.HandleFunc("/api/urls", shortener.createURL)
	http.HandleFunc("/api/urls/", shortener.deleteURL)
	http.HandleFunc("/api/stats/", shortener.getStats)
	http.HandleFunc("/", shortener.redirect)

	log.Println("Server starting on :8000")
	if err := http.ListenAndServe(":8000", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}