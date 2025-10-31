// server.js (UPDATED: Removed cluster and initial fetch)

// 1. Import modules
const express = require('express');
const path = require('path');
const bodyParser = require('body-parser');
const basicAuth = require('express-basic-auth'); 
const aggregator = require('./aggregator');
const db = require('./database');
const curator = require('./curator');

// 2. Initialize the app and set the port
const app = express();
const PORT = process.env.PORT || 3000;


// --- MIDDLEWARE SETUP ---
app.use(bodyParser.json());
app.use(express.static('public')); 

// --- Basic Authentication Middleware ---
const adminUser = process.env.ADMIN_USER || 'admin';
const adminPass = process.env.ADMIN_PASSWORD;

if (!adminPass) {
    console.error("CRITICAL: ADMIN_PASSWORD environment variable is not set. Admin panel will not be accessible.");
}

const adminAuth = basicAuth({
    users: { [adminUser]: adminPass },
    challenge: true,
    unauthorizedResponse: 'Access Denied. Please check your credentials.'
});

// --- API ROUTES (Endpoints) ---

// Fetch all stored news articles
app.get('/api/news', async (req, res) => {
    console.log("--> Received request for /api/news");
    try {
        const articles = await db.getAllArticles();
        console.log(`--> Found ${articles.length} articles in DB.`);
        res.json(articles);
    } catch (error) {
        console.error("!!! ERROR in /api/news:", error);
        res.status(500).json({ error: 'Failed to retrieve articles.' });
    }
});

// Endpoint 1: Generate AI Text
app.post('/api/generate-text', async (req, res) => {
    const article = req.body;
    if (!article || !article.title) {
        return res.status(400).json({ error: 'Missing article data.' });
    }
    try {
        const curatedText = await curator.generateAiText(article);
        res.json(curatedText);
    } catch (error) {
        console.error("Error during text generation:", error);
        res.status(500).json({ error: 'Text generation failed.' });
    }
});

// Endpoint 2: Extract Keywords
app.post('/api/extract-keywords', async (req, res) => {
    const { headline, description } = req.body;
    if (!headline || !description) {
        return res.status(400).json({ error: 'Missing headline or description.' });
    }
    try {
        const keywords = await curator.extractSearchKeywords(headline, description);
        res.json({ keywords });
    } catch (error) {
        console.error("Error during keyword extraction:", error);
        res.status(500).json({ error: 'Keyword extraction failed.' });
    }
});

// Endpoint 3: Get Alternative Keywords
app.post('/api/get-alternative-keywords', async (req, res) => {
    const { headline, description, previousKeywords } = req.body;
    if (!headline || !description) {
        return res.status(400).json({ error: 'Missing headline or description.' });
    }
    const prevKeywordsArray = Array.isArray(previousKeywords) ? previousKeywords : [];
    try {
        const keywords = await curator.getAlternativeKeywords(headline, description, prevKeywordsArray);
        res.json({ keywords });
    } catch (error) {
        console.error("Error during alternative keyword extraction:", error);
        res.status(500).json({ error: 'Alternative keyword extraction failed.' });
    }
});


// Endpoint 4: Search for Images
app.post('/api/search-images', async (req, res) => {
    const { query, startIndex } = req.body;
    if (!query) {
        return res.status(400).json({ error: 'Missing query.' });
    }
    const index = parseInt(startIndex, 10) || 0;
    try {
        const imagesData = await curator.searchForRelevantImages(query, index);
        res.json({ images: imagesData });
    } catch (error) {
        console.error("Error during image search:", error);
        res.status(500).json({ error: 'Image search failed.' });
    }
});

// Endpoint 5: Find Related Articles
app.post('/api/find-related-articles', async (req, res) => {
    const { title, source } = req.body;
    if (!title || !source) {
        return res.status(400).json({ error: 'Missing title or source.' });
    }
    try {
        const relatedArticles = await curator.findRelatedWebArticles(title, source);
        res.json({ relatedArticles });
    } catch (error) {
        console.error("Error during related article search:", error);
        res.status(500).json({ error: 'Related article search failed.' });
    }
});

// Endpoint 6: Find Video
app.post('/api/find-video', async (req, res) => {
    const { title, source } = req.body;
    if (!title || !source) {
        return res.status(400).json({ error: 'Missing title or source.' });
    }
    try {
        const videoUrl = await curator.findRelatedVideo(title, source);
        res.json({ videoUrl });
    } catch (error) {
        console.error("Error during video search:", error);
        res.status(500).json({ error: 'Video search failed.' });
    }
});

// *** MODIFIED ENDPOINT ***
// Simple Preview Image Generation
app.post('/api/generate-simple-preview', async (req, res) => {
    const { imageUrl, overlayText } = req.body; 
    if (!imageUrl || !overlayText) {
        return res.status(400).json({ error: 'Missing data for preview.' });
    }
    try {
        // This function now returns a buffer or null
        const imageBuffer = await curator.generateSimplePreviewImage(imageUrl, overlayText); 
        
        if (imageBuffer) {
            // Set the correct content type and send the buffer directly
            res.set('Content-Type', 'image/png');
            res.send(imageBuffer);
        } else {
            // Send a 500 error if the buffer is null (generation failed)
            res.status(500).json({ error: 'Preview generation failed on server.' });
        }
    } catch (error) {
        console.error("!!! ERROR in /api/generate-simple-preview:", error);
        res.status(500).json({ error: 'Internal server error.' });
    }
});

// Social Media Sharing (MOCK-UP)
app.post('/api/share', async (req, res) => {
    const { imagePath, caption, platform } = req.body;
    console.log(`\n*** MOCK SHARE REQUEST to ${platform} ***\n`);
    res.json({ success: true, message: `Successfully simulated sharing to ${platform}!` });
});

// Add a new link (from admin panel)
app.post('/api/links/add', async (req, res) => {
    console.log("--> Received request for /api/links/add");
    const { title, link } = req.body;
    if (!title || !link) {
        return res.status(400).json({ success: false, error: 'Missing title or link.' });
    }
    try {
        await db.addLink(title, link);
        res.json({ success: true, message: 'Link added!' });
    } catch (error) {
        console.error("!!! ERROR in /api/links/add:", error);
        res.status(500).json({ success: false, error: 'Failed to add link.' });
    }
});

// Get all links (for public page)
app.get('/api/links/get', async (req, res) => {
    console.log("--> Received request for /api/links/get");
    try {
        const links = await db.getAllLinks();
        res.json(links);
    } catch (error) {
        console.error("!!! ERROR in /api/links/get:", error);
        res.status(500).json({ error: 'Failed to retrieve links.' });
    }
});

// --- PAGE ROUTING ---

// Public root: Serves the public "link in bio" page
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'links.html'));
});

// Admin panel: Serves the private curator app, protected by password
app.get('/admin', adminAuth, (req, res) => {
    if (!adminPass) {
        return res.status(500).send("Server is not configured with an ADMIN_PASSWORD. Access denied.");
    }
    res.sendFile(path.join(__dirname, 'admin', 'index.html'));
});

// --- SERVER START FUNCTION ---
// This function is now run by a single process
async function startApp() {
    try {
        await db.connectDB();
        console.log("Database connected successfully.");

        app.listen(PORT,'0.0.0.0', () => {
            console.log(`Server running at http://localhost:${PORT}`);
            
            // Start the recurring schedule
            aggregator.startScheduler();
            
            // ------------------
            // --- FIX APPLIED ---
            // ------------------
            // Removed the initial fetch to prevent startup timeout
            // console.log("Staging initial news fetch in 10 seconds...");
            // setTimeout(aggregator.runFetch, 10000); // <-- REMOVED
        });
    } catch (dbError) {
        console.error("Server failed to start:", dbError);
        process.exit(1);
    }
}

// --- INITIATE SERVER START ---
// No more cluster logic, just run the app
startApp();
