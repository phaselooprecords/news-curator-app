// database.js (Updated DB Name)

require('dotenv').config();
const { MongoClient } = require('mongodb');

// Get variables from .env file
const uri = process.env.MONGO_URI;
const dbName = "newsCuratorDB"; // <<<--- UPDATED NAME

if (!uri) {
    throw new Error("MONGO_URI environment variable not set. Check your .env file.");
}

const client = new MongoClient(uri);
let db;

// --- CONNECTION FUNCTION ---
async function connectDB() {
    try {
        console.log("Connecting to MongoDB Atlas...");
        await client.connect();
        db = client.db(dbName);
        console.log(`Successfully connected to MongoDB database: ${dbName}!`);

    } catch (error) {
        console.error("Database connection failed:", error.message);
        process.exit(1);
    }
}

// --- CORE OPERATIONS ---

// Function to save an array of news articles to the 'articles' collection
async function insertArticles(articles) {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('articles'); // Still using 'articles' collection

    // Use bulkWrite for efficiency: update if link exists, insert otherwise
    const operations = articles.map(article => ({
        updateOne: {
            filter: { link: article.link }, // Unique identifier
            update: { $set: { ...article, fetchedAt: new Date() } }, // Update content and add fetch timestamp
            upsert: true // Insert if doesn't exist
        }
    }));

    if (operations.length > 0) {
        try {
            const result = await collection.bulkWrite(operations);
            console.log(`[DB] Inserted/Updated: ${result.upsertedCount + result.modifiedCount} articles.`);
        } catch (error) {
            console.error("[DB ERROR] Failed to perform bulk write:", error.message);
        }
    }
}

// Function to fetch all stored articles for the frontend display
async function getAllArticles() {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('articles');
    // Sort by publication date, newest first
    return await collection.find({}).sort({ pubDate: -1 }).toArray();
}


// --- EXPORTS ---
module.exports = {
    connectDB,
    insertArticles,
    getAllArticles
};