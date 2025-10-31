// fetch-worker.js
// This file runs in a separate thread.

const { parentPort } = require('worker_threads');
const Parser = require('rss-parser');
const db = require('./database');

// --- All the fetch logic is moved here from aggregator.js ---
const parser = new Parser({
    customFields: {
      item: [['media:content', 'media:content', {keepArray: false}]],
    }
});

const RSS_FEEDS_MASTER = [
  // --- 📰 Top-Level World & US News ---
  { name: 'Reuters - Top News', url: 'http://feeds.reuters.com/reuters/topNews' },
  { name: 'Reuters - World News', url: 'http://feeds.reuters.com/Reuters/worldNews' },
  { name: 'Associated Press - Top News', url: 'https://apnews.com/rss' },
  { name: 'BBC News - World', url: 'http://feeds.bbci.co.uk/news/world/rss.xml' },
  { name: 'New York Times - Home Page', url: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml' },
  { name: 'The Guardian - World', url: 'https://www.theguardian.com/world/rss' },
  { name: 'NPR - News', url: 'https://feeds.npr.org/1001/rss.xml' },
  { name: 'Al Jazeera - English', url: 'https://www.aljazeera.com/xml/rss/all.xml' },
  { name: 'ABC News - Top Stories', url: 'https://abcnews.go.com/abcnews/topstories' },
  { name: 'NBC News - Top Stories', url: 'http://feeds.nbcnews.com/nbcnews/public/news' },
  { name: 'CBS News - Main', url: 'https://www.cbsnews.com/latest/rss/main' },
  { name: 'LA Times - Top News', url: 'https://www.latimes.com/world-nation/rss2.0.xml' },

  // --- 🌍 International & Regional News ---
  { name: 'Deutsche Welle (DW) - All', url: 'https://rss.dw.com/rdf/rss-en-all' },
  { name: 'Le Monde - International (English)', url: 'https://www.lemonde.fr/en/international/rss_full.xml' },
  { name: 'Times of India - Top Stories', url: 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms' },
  { name: 'BBC News - Asia', url: 'http://feeds.bbci.co.uk/news/world/asia/rss.xml' },
  { name: 'BBC News - Europe', url: 'http://feeds.bbci.co.uk/news/world/europe/rss.xml' },
  { name: 'Axios - World', url: 'https://api.axios.com/feed/world' },
  { name: 'Foreign Policy', url: 'https://foreignpolicy.com/feed/' },

  // --- 🏛️ Politics & Policy ---
  { name: 'Politico', url: 'https://rss.politico.com/politics-news.xml' },
  { name: 'The Hill', url: 'https://thehill.com/rss/syndicator/19109' },
  { name: 'NPR - Politics', url: 'https://feeds.npr.org/1014/rss.xml' },
  { name: 'New York Times - Politics', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml' },

  // --- 💼 Business & Finance ---
  { name: 'Wall Street Journal - Business', url: 'https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml' },
  { name: 'The Economist - Business', url: 'https://www.economist.com/business/rss.xml' },
  { name: 'Financial Times - Home (UK)', url: 'https://www.ft.com/rss/home/uk' },
  { name: 'Bloomberg - Top News', url: 'https://feeds.bloomberg.com/windows/rss.xml' },
  { name: 'CNBC - Top News', url: 'https://www.cnbc.com/id/100003114/device/rss/rss.html' },
  { name: 'Harvard Business Review', url: 'https://hbr.org/feed' },
  { name: 'Forbes', url: 'https://www.forbes.com/rss/' },
  { name: 'MarketWatch - Top Stories', url: 'http://feeds.marketwatch.com/marketwatch/topstories/' },

  // --- 💻 Technology (General) ---
  { name: 'TechCrunch', url: 'https://techcrunch.com/feed/' },
  { name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml' },
  { name: 'Ars Technica', url: 'http://feeds.arstechnica.com/arstechnica/index' },
  { name: 'Wired', url: 'https://www.wired.com/feed/rss' },
  { name: 'Hacker News', url: 'https://news.ycombinator.com/rss' },
  { name: 'ZDNet', url: 'https://www.zdnet.com/feed/' },
  { name: 'Engadget', url: 'https://www.engadget.com/rss.xml' },
  { name: 'MIT Technology Review', url: 'https://www.technologyreview.com/feed/' },
  { name: 'Techmeme', url: 'https://www.techmeme.com/feed.xml' },

  // --- 🤖 AI & Cybersecurity ---
  { name: 'Google AI Blog', url: 'https://research.google/blog/rss/' },
  { name: 'The Hacker News', url: 'http://feeds.feedburner.com/TheHackersNews' },
  { name: 'Krebs on Security', url: 'https://krebsonsecurity.com/feed/' },
  { name: 'Schneier on Security', url: 'https://www.schneier.com/feed/atom/' },
  { name: 'Dark Reading', url: 'https://www.darkreading.com/rss_simple.asp' },
  { name: 'Bleeping Computer', url: 'https://www.bleepingcomputer.com/feed/' },

  // --- 🔬 Science & Space ---
  { name: 'NASA - Breaking News', url: 'https://www.nasa.gov/rss/dyn/breaking_news.rss' },
  { name: 'Nature', url: 'https://www.nature.com/nature.rss' },
  { name: 'Scientific American', url: 'https://www.scientificamerican.com/feed/rss.cfm' },
  { name: 'ScienceDaily', url: 'http://feeds.sciencedaily.com/sciencedaily' },
  { name: 'Quanta Magazine', url: 'httpsD' },

  // --- 🕵️ Investigative & Fact-Checking ---
  { name: 'ProPublica', url: 'http://feeds.propublica.org/propublica/main' },
  { name: 'Snopes', url: 'https://www.snopes.com/feed/' },
  { name: 'The Intercept', url: 'https://theintercept.com/feed/?lang=en' },

  // --- 🎙️ Popular Podcasts (as Feeds) ---
  { name: '99% Invisible', url: 'http://feeds.99percentinvisible.org/99percentinvisible' },
  { name: 'This American Life', url: 'http://feeds.thisamericanlife.org/talpodcast' },
  { name: 'Radiolab', url: 'http://feeds.wnyc.org/radiolab' },
  { name: 'Freakonomics Radio', url: 'http://feeds.feedburner.com/freakonomicsradio' },
];


async function fetchAndProcessNews() {
    console.log(`\n[Worker] --- Starting news fetch at ${new Date().toLocaleTimeString()} ---`);
    let collectedArticles = [];

    for (const feed of RSS_FEEDS_MASTER) {
        try {
            let rss = await parser.parseURL(feed.url);
            
            const processedItems = rss.items.map(item => {
                // Find the original image URL
                let originalImageUrl = null;
                if (item.enclosure && item.enclosure.url && item.enclosure.type.startsWith('image')) {
                    originalImageUrl = item.enclosure.url;
                } else if (item['media:content'] && item['media:content'].$.url) {
                    originalImageUrl = item['media:content'].$.url;
                }

                return {
                    source: feed.name,
                    title: item.title,
                    link: item.link,
                    pubDate: item.pubDate ? new Date(item.pubDate) : new Date(),
                    originalImageUrl: originalImageUrl 
                };
            }).slice(0, 5); // Get the 5 most recent

            collectedArticles.push(...processedItems);
        } catch (error) {
            // Log only if it's NOT a common 404/403/ENOTFOUND/429 error
            if (!error.message.includes('Status code 404') && 
                !error.message.includes('Status code 403') &&
                !error.message.includes('Status code 429') &&
                !error.message.includes('ENOTFOUND') &&
                !error.message.includes('socket hang up') &&
                !error.message.includes('Invalid character')) {
                console.error(`[Worker ERROR] Failed to fetch feed for ${feed.name}: ${error.message}`);
            }
        }
    }
    
    collectedArticles.sort((a, b) => b.pubDate.getTime() - a.pubDate.getTime());
    
    if (collectedArticles.length > 0) {
        await db.insertArticles(collectedArticles); 
    }
    
    console.log(`[Worker] Total ${collectedArticles.length} items processed.`);
    console.log('[Worker] --- News fetch complete ---');

    return collectedArticles.length;
}

// Main worker function
const runWorker = async () => {
    try {
        // Connect to DB *within the worker*
        await db.connectDB();
        console.log("[Worker] Database connected.");
        const count = await fetchAndProcessNews();
        // Send a success message back
        parentPort.postMessage({ status: 'done', count: count });
    } catch (error) {
        console.error("[Worker CRITICAL ERROR]", error);
        parentPort.postMessage({ status: 'error', error: error.message });
    }
};

// Start the process
runWorker();
