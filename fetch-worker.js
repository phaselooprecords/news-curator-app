// fetch-worker.js
// This file runs in a separate thread.

const { parentPort } = require('worker_threads');
const Parser = require('rss-parser');
const db = require('./database');

// --- Helper function for throttling ---
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));
const DELAY_PER_FEED_MS = 250; // 250ms pause between each feed fetch

// --- All the fetch logic is moved here from aggregator.js ---
const parser = new Parser({
    customFields: {
      item: [['media:content', 'media:content', {keepArray: false}]],
    }
});

// --- UPDATED: Added 'category' field to each feed ---
const RSS_FEEDS_MASTER = [
  // --- 📰 Top-Level World & US News ---
  { name: 'Reuters - Top News', url: 'http://feeds.reuters.com/reuters/topNews', category: 'Daily' },
  { name: 'Reuters - World News', url: 'http://feeds.reuters.com/Reuters/worldNews', category: 'Daily' },
  { name: 'Associated Press - Top News', url: 'https://apnews.com/rss', category: 'Daily' },
  { name: 'BBC News - World', url: 'http://feeds.bbci.co.uk/news/world/rss.xml', category: 'Daily' },
  { name: 'New York Times - Home Page', url: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', category: 'Daily' },
  { name: 'The Guardian - World', url: 'https://www.theguardian.com/world/rss', category: 'Daily' },
  { name: 'NPR - News', url: 'https://feeds.npr.org/1001/rss.xml', category: 'Daily' },
  { name: 'Al Jazeera - English', url: 'https://www.aljazeera.com/xml/rss/all.xml', category: 'Daily' },
  { name: 'ABC News - Top Stories', url: 'https://abcnews.go.com/abcnews/topstories', category: 'Daily' },
  { name: 'NBC News - Top Stories', url: 'http://feeds.nbcnews.com/nbcnews/public/news', category: 'Daily' },
  { name: 'CBS News - Main', url: 'https://www.cbsnews.com/latest/rss/main', category: 'Daily' },
  { name: 'LA Times - Top News', url: 'https://www.latimes.com/world-nation/rss2.0.xml', category: 'Daily' },

  // --- 🌍 International & Regional News ---
  { name: 'Deutsche Welle (DW) - All', url: 'https://rss.dw.com/rdf/rss-en-all', category: 'Daily' },
  { name: 'Le Monde - International (English)', url: 'https://www.lemonde.fr/en/international/rss_full.xml', category: 'Daily' },
  { name: 'Times of India - Top Stories', url: 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms', category: 'Daily' },
  { name: 'BBC News - Asia', url: 'http://feeds.bbci.co.uk/news/world/asia/rss.xml', category: 'Daily' },
  { name: 'BBC News - Europe', url: 'http://feeds.bbci.co.uk/news/world/europe/rss.xml', category: 'Daily' },
  { name: 'Axios - World', url: 'https://api.axios.com/feed/world', category: 'Daily' },
  { name: 'Foreign Policy', url: 'https://foreignpolicy.com/feed/', category: 'Politics' },

  // --- 🏛️ Politics & Policy ---
  { name: 'Politico', url: 'https://rss.politico.com/politics-news.xml', category: 'Politics' },
  { name: 'The Hill', url: 'https://thehill.com/rss/syndicator/19109', category: 'Politics' },
  { name: 'NPR - Politics', url: 'https://feeds.npr.org/1014/rss.xml', category: 'Politics' },
  { name: 'New York Times - Politics', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml', category: 'Politics' },

  // --- 💼 Business & Finance ---
  { name: 'Wall Street Journal - Business', url: 'https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml', category: 'Business' },
  { name: 'The Economist - Business', url: 'https://www.economist.com/business/rss.xml', category: 'Business' },
  { name: 'Financial Times - Home (UK)', url: 'https://www.ft.com/rss/home/uk', category: 'Business' },
  { name: 'Bloomberg - Top News', url: 'https://feeds.bloomberg.com/windows/rss.xml', category: 'Business' },
  { name: 'CNBC - Top News', url: 'https://www.cnbc.com/id/100003114/device/rss/rss.html', category: 'Business' },
  { name: 'Harvard Business Review', url: 'https://hbr.org/feed', category: 'Business' },
  { name: 'Forbes', url: 'https://www.forbes.com/rss/', category: 'Business' },
  { name: 'MarketWatch - Top Stories', url: 'http://feeds.marketwatch.com/marketwatch/topstories/', category: 'Business' },

  // --- 💻 Technology (General) ---
  { name: 'TechCrunch', url: 'https://techcrunch.com/feed/', category: 'Tech' },
  { name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml', category: 'Tech' },
  { name: 'Ars Technica', url: 'http://feeds.arstechnica.com/arstechnica/index', category: 'Tech' },
  { name: 'Wired', url: 'https://www.wired.com/feed/rss', category: 'Tech' },
  { name: 'Hacker News', url: 'https://news.ycombinator.com/rss', category: 'Tech' },
  { name: 'ZDNet', url: 'https://www.zdnet.com/feed/', category: 'Tech' },
  { name: 'Engadget', url: 'https://www.engadget.com/rss.xml', category: 'Tech' },
  { name: 'MIT Technology Review', url: 'https://www.technologyreview.com/feed/', category: 'Tech' },
  { name: 'Techmeme', url: 'https://www.techmeme.com/feed.xml', category: 'Tech' },

  // --- 🤖 AI & Cybersecurity ---
  { name: 'Google AI Blog', url: 'https://research.google/blog/rss/', category: 'Tech' },
  { name: 'The Hacker News', url: 'http://feeds.feedburner.com/TheHackersNews', category: 'Tech' },
  { name: 'Krebs on Security', url: 'https://krebsonsecurity.com/feed/', category: 'Tech' },
  { name: 'Schneier on Security', url: 'https://www.schneier.com/feed/atom/', category: 'Tech' },
  { name: 'Dark Reading', url: 'https://www.darkreading.com/rss_simple.asp', category: 'Tech' },
  { name: 'Bleeping Computer', url: 'https://www.bleepingcomputer.com/feed/', category: 'Tech' },

  // --- 🔬 Science & Space ---
  { name: 'NASA - Breaking News', url: 'https://www.nasa.gov/rss/dyn/breaking_news.rss', category: 'Science' },
  { name: 'Nature', url: 'https://www.nature.com/nature.rss', category: 'Science' },
  { name: 'Scientific American', url: 'https://www.scientificamerican.com/feed/rss.cfm', category: 'Science' },
  { name: 'ScienceDaily', url: 'http://feeds.sciencedaily.com/sciencedaily', category: 'Science' },
  { name: 'Quanta Magazine', url: 'https://api.quantamagazine.org/feed/', category: 'Science' },
  { name: 'Space.com', url: 'https://www.space.com/feeds/all', category: 'Science' },

  // --- 🩺 Health & Medicine ---
  { name: 'STAT News', url: 'https://www.statnews.com/feed/', category: 'Health' },
  { name: 'MedPage Today', url: 'https://www.medpagetoday.com/rss/headlines.xml', category: 'Health' },
  { name: 'NPR - Health', url: 'https://feeds.npr.org/1007/rss.xml', category: 'Health' },
  { name: 'New York Times - Health', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Health.xml', category: 'Health' },

  // --- 🌿 Environment & Climate ---
  { name: 'Grist', url: 'https://grist.org/feed/', category: 'Nature' },
  { name: 'Inside Climate News', url: 'https://insideclimatenews.org/feed/', category: 'Nature' },
  { name: 'The Guardian - Environment', url: 'https://www.theguardian.com/environment/rss', category: 'Nature' },

  // --- 🤔 Culture, Opinion & Long-form ---
  { name: 'The Atlantic', url: 'https://www.theatlantic.com/feed/all/', category: 'Culture' },
  { name: 'The New Yorker', url: 'https://www.newyorker.com/feed/everything', category: 'Culture' },
  { name: 'Aeon', url: 'https://aeon.co/feed.rss', category: 'Culture' },
  { name: 'NYT - Opinion', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml', category: 'Culture' },
  { name: 'The Guardian - Opinion', url: 'https://www.theguardian.com/commentisfree/rss', category: 'Culture' },

  // --- 🎬 Entertainment & Fandom ---
  { name:'Variety', url: 'https://variety.com/feed/', category: 'Entertainment' },
  { name: 'The Hollywood Reporter', url: 'https://www.hollywoodreporter.com/feed/', category: 'Entertainment' },
  { name: 'Deadline', url: 'https://deadline.com/feed/', category: 'Entertainment' },
  { name: 'Comic Book Resources (CBR)', url: 'https://www.cbr.com/feed/', category: 'Entertainment' },
  { name: 'The Mary Sue', url: 'https://www.themarysue.com/feed/', category: 'Entertainment' },

  // --- 🎨 Creative: Art & Photography ---
  { name: 'Colossal (Art)', url: 'http://feeds.feedburner.com/colossal', category: 'Art' },
  { name: 'Hyperallergic', url: 'https://hyperallergic.com/feed/', category: 'Art' },
  { name: 'PetaPixel (Photography)', url: 'https://petapixel.com/feed/', category: 'Art' },
  { name: 'Booooooom', url: 'https://www.booooooom.com/feed/', category: 'Art' },

  // --- 🏠 Creative: Design & Architecture ---
  { name: 'Dezeen', url: 'http://feeds.feedburner.com/dezeen', category: 'Art' },
  { name: 'designboom', url: 'https://www.designboom.com/feed/', category: 'Art' },
  { name: 'Swissmiss', url: 'https://www.swiss-miss.com/feed', category: 'Art' },
  { name: 'Curbed', url: 'https://www.curbed.com/rss/index.xml', category: 'Art' },
  
  // --- 👟 Creative: Fashion & Style ---
  { name: 'Vogue', url: 'https://www.vogue.com/feed/rss', category: 'Culture' },
  { name: 'Hypebeast', url: 'https://hypebeast.com/feed', category: 'Culture' },

  // --- 🍔 Hobbies: Food & Cooking ---
  { name: 'Eater (All)', url: 'https://www.eater.com/rss/index.xml', category: 'Culture' },
  { name: 'Bon Appétit', url: 'https://www.bonappetit.com/feed/rss', category: 'Culture' },
  { name: 'Smitten Kitchen', url: 'http://feeds.feedburner.com/SmittenKitchen', category: 'Culture' },

  // --- 🎮 Hobbies: Video Games ---
  { name: 'Kotaku', url: 'https://kotaku.com/rss', category: 'Entertainment' },
  { name: 'IGN', url: 'http://feeds.ign.com/ign/all', category: 'Entertainment' },
  { name: 'Game Rant', url: 'https://gamerant.com/feed/', category: 'Entertainment' },
  { name: 'GameSpot - All News', url: 'https://www.gamespot.com/feeds/news/', category: 'Entertainment' },
  { name: 'Polygon', url: 'https://www.polygon.com/rss/index.xml', category: 'Entertainment' },

  // --- ⚽ Hobbies: Sports ---
  { name: 'ESPN', url: 'https://www.espn.com/espn/rss/news', category: 'Sports' },
  { name: 'BBC Sport', url: 'http://feeds.bbci.co.uk/sport/rss.xml', category: 'Sports' },
  { name: 'SB Nation', url: 'httpss://www.sbnation.com/rss/index.xml', category: 'Sports' },

  // --- 🚗 Hobbies: Automotive ---
  { name: 'Jalopnik', url: 'https://jalopnik.com/rss', category: 'Tech' },

  // --- 📚 Intellectual: History, Philosophy, Literature ---
  { name: 'Daily Stoic', url: 'https://dailystoic.com/feed/', category: 'Culture' },

  // --- 💡 Lifestyle & Productivity ---
  { name: 'Lifehacker', url: 'https://lifehacker.com/rss', category: 'Culture' },
  { name: 'Fast Company', url: 'https://www.fastcompany.com/rss', category: 'Business' },
  { name: 'WIRED - Ideas', url: 'httpsS://www.wired.com/feed/category/ideas/latest/rss', category: 'Culture' },

  // --- 🕵️ Investigative & Fact-Checking ---
  { name: 'ProPublica', url: 'http://feeds.propublica.org/propublica/main', category: 'Politics' },
  { name: 'Snopes', url: 'https://www.snopes.com/feed/', category: 'Daily' },
  { name: 'The Intercept', url: 'https://theintercept.com/feed/?lang=en', category: 'Politics' },

  // --- 🎙️ Popular Podcasts (as Feeds) ---
  { name: '99% Invisible', url: 'http://feeds.99percentinvisible.org/99percentinvisible', category: 'Art' },
  { name: 'This American Life', url: 'http://feeds.thisamericanlife.org/talpodcast', category: 'Culture' },
  { name: 'Radiolab', url: 'http://feeds.wnyc.org/radiolab', category: 'Science' },
  { name: 'Freakonomics Radio', url: 'http://feeds.feedburner.com/freakonomicsradio', category: 'Business' },
];


async function fetchAndProcessNews() {
    console.log(`\n[Worker] --- Starting news fetch at ${new Date().toLocaleTimeString()} ---`);
    let collectedArticles = [];
    let feedCount = 0;
    const totalFeeds = RSS_FEEDS_MASTER.length;

    for (const feed of RSS_FEEDS_MASTER) {
        feedCount++;
        console.log(`[Worker] Fetching feed ${feedCount}/${totalFeeds}: ${feed.name}`);
        try {
            // *** TEMPORARY FIX: Manually correct URL if it's bad ***
            let feedUrl = feed.url;
            if (feedUrl.startsWith('httpss://')) {
                feedUrl = feedUrl.replace('httpss://', 'https://');
            }

            let rss = await parser.parseURL(feedUrl); // Use the corrected URL
            
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
                    category: feed.category || 'General', // <-- Added category
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
                !error.message.includes('Invalid character') &&
                !error.message.includes('Protocol "httpss:"') // No need to log this one anymore
                ) {
                console.error(`[Worker ERROR] Failed to fetch feed for ${feed.name}: ${error.message}`);
            }
        }

        // Wait for a short time after each feed to smooth out the CPU/network load
        await delay(DELAY_PER_FEED_MS);
    }
    
    collectedArticles.sort((a, b) => b.pubDate.getTime() - a.pubDate.getTime());
    
    if (collectedArticles.length > 0) {
        // The single bulkWrite at the end is efficient and fine.
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
