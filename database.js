// database.js (UPDATED with deleteLink function)

require('dotenv').config(); 
const { MongoClient, ObjectId } = require('mongodb'); // <-- Added ObjectId

// Get variables from .env file
const uri = process.env.MONGO_URI; 
const dbName = "musiccuratorDB"; // Switched back to your DB name

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

// --- CORE OPERATIONS ('articles' collection) ---

// Function to save an array of news articles to the 'articles' collection
async function insertArticles(articles) {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('articles');

    const operations = articles.map(article => ({
        updateOne: {
            filter: { link: article.link },
            update: { $set: { ...article, fetchedAt: new Date() } },
            upsert: true
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
    return await collection.find({}).sort({ pubDate: -1 }).toArray();
}

// --- "LINKS" COLLECTION FUNCTIONS ---

/**
 * Adds a new link to the 'links' collection.
 */
async function addLink(title, link) {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('links');
    try {
        const result = await collection.updateOne(
            { link: link }, // Filter by link to prevent duplicates
            { $set: { title: title, link: link, createdAt: new Date() } }, // Set data
            { upsert: true } // Insert if it doesn't exist
        );
        if (result.upsertedCount > 0) {
            console.log(`[DB] Added new link: ${title}`);
        } else {
            console.log(`[DB] Updated existing link: ${title}`);
        }
    } catch (error) {
        console.error("[DB ERROR] Failed to add link:", error.message);
        throw error; // Re-throw to be caught by server.js
    }
}

/**
 * Retrieves all links from the 'links' collection, sorted newest first.
 */
async function getAllLinks() {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('links');
    return await collection.find({}).sort({ createdAt: -1 }).toArray();
}

/**
 * *** NEW FUNCTION ***
 * Deletes a link from the 'links' collection by its ID.
 */
async function deleteLink(linkId) {
    if (!db) {
        throw new Error("Database not connected.");
    }
    const collection = db.collection('links');
    try {
        // MongoDB _id must be an ObjectId
        const result = await collection.deleteOne({ _id: new ObjectId(linkId) });
        console.log(`[DB] Deleted link, count: ${result.deletedCount}`);
        return result;
    } catch (error) {
        console.error("[DB ERROR] Failed to delete link:", error.message);
        if (error.message.includes("Argument passed in must be a string of 12 bytes")) {
            console.error("Error: Invalid Link ID format.");
            return { deletedCount: 0 }; // Return 0 if ID was invalid
        }
        throw error;
    }
}

// --- EXPORTS ---
module.exports = {
    connectDB,
    insertArticles,
    getAllArticles,
    addLink,
    getAllLinks,
    deleteLink // <-- NEW EXPORT
};
