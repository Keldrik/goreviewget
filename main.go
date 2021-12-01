package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ReviewRequest struct {
	Count   int      `json:"count"`
	Reviews []Review `json:"results"`
}

type Review struct {
	ShopID            int    `bson:"shopid,omitempty" json:"shop_id"`
	ListingID         int    `bson:"listingid,omitempty" json:"listing_id"`
	TransactionID     int    `bson:"transactionid,omitempty" json:"transaction_id"`
	BuyerUserID       int    `bson:"buyerid,omitempty" json:"buyer_user_id"`
	Rating            int    `bson:"rating,omitempty" json:"rating"`
	Review            string `bson:"review,omitempty" json:"review"`
	Language          string `bson:"language,omitempty" json:"language"`
	ImageURLFullxfull string `bson:"imageurl,omitempty" json:"image_url_fullxfull"`
	CreateTimestamp   int    `bson:"created,omitempty" json:"create_timestamp"`
	UpdateTimestamp   int    `bson:"updated,omitempty" json:"update_timestamp"`
}

//var mongoUrl = "mongodb://localhost"
var mongoUrl = os.Getenv("MONGOURL")
var shopId = os.Getenv("SHOPID")
var apiKey = os.Getenv("APIKEY")
var downloadedReviews []Review

func getReviewsFromEtsy(pageIndex int, pageSize int) {
	fmt.Printf("Get Etsy API Reviews Page %d with Page Size %d...\n", pageIndex+1, pageSize)
	req, _ := http.NewRequest("GET", fmt.Sprintf("https://openapi.etsy.com/v3/application/shops/%s/reviews?limit=%d&offset=%d", shopId, pageSize, pageIndex*pageSize), nil)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("x-api-key", apiKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var reviewRequest ReviewRequest
	err = json.NewDecoder(resp.Body).Decode(&reviewRequest)
	if err != nil {
		panic(err)
	}
	downloadedReviews = append(downloadedReviews, reviewRequest.Reviews...)
	fmt.Printf("%d Reviews added!\n", len(reviewRequest.Reviews))
	if len(reviewRequest.Reviews) > 0 {
		time.Sleep(200 * time.Millisecond)
		getReviewsFromEtsy(pageIndex+1, pageSize)
	}
}

func saveToDatabase() {
	fmt.Printf("Connecting to database...\n")
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoUrl))
	if err != nil {
		panic(err)
	}
	defer client.Disconnect(ctx)
	database := client.Database("bbpcontent")
	reviewsCollection := database.Collection("reviews")
	fmt.Printf("Dropping old Collection...\n")
	err = reviewsCollection.Drop(ctx)
	if err != nil {
		panic(err)
	}
	var mongoReviews []interface{}
	for _, r := range downloadedReviews {
		if (len(r.Review) > 0 || len(r.ImageURLFullxfull) > 0) && r.Rating >= 4 {
			mongoReviews = append(mongoReviews, r)
		}
	}
	fmt.Printf("Saving Reviews to Database...\n")
	result, err := reviewsCollection.InsertMany(ctx, mongoReviews)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%d Reviews saved to Database!\n", len(result.InsertedIDs))
}

func main() {
	getReviewsFromEtsy(0, 100)
	fmt.Printf("\n%d Reviews in collection!\n\n", len(downloadedReviews))
	saveToDatabase()
	fmt.Printf("\nDONE!")
}
