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

const RSS_FEEDS_MASTER = [
  // --- 📰 Top-Level World & US News ---
  { name: 'Reuters - Top News', url: 'http://feeds.reuters.com/reuters/topNews', category: '📰 Top-Level World & US News' },
  { name: 'Reuters - World News', url: 'http://feeds.reuters.com/Reuters/worldNews', category: '📰 Top-Level World & US News' },
  { name: 'Associated Press - Top News', url: 'https://apnews.com/rss', category: '📰 Top-Level World & US News' },
  { name: 'BBC News - World', url: 'http://feeds.bbci.co.uk/news/world/rss.xml', category: '📰 Top-Level World & US News' },
  { name: 'New York Times - Home Page', url: 'https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml', category: '📰 Top-Level World & US News' },
  { name: 'The Guardian - World', url: 'https://www.theguardian.com/world/rss', category: '📰 Top-Level World & US News' },
  { name: 'NPR - News', url: 'https://feeds.npr.org/1001/rss.xml', category: '📰 Top-Level World & US News' },
  { name: 'Al Jazeera - English', url: 'https://www.aljazeera.com/xml/rss/all.xml', category: '📰 Top-Level World & US News' },
  { name: 'ABC News - Top Stories', url: 'https://abcnews.go.com/abcnews/topstories', category: '📰 Top-Level World & US News' },
  { name: 'NBC News - Top Stories', url: 'http://feeds.nbcnews.com/nbcnews/public/news', category: '📰 Top-Level World & US News' },
  { name: 'CBS News - Main', url: 'https://www.cbsnews.com/latest/rss/main', category: '📰 Top-Level World & US News' },
  { name: 'LA Times - Top News', url: 'https://www.latimes.com/world-nation/rss2.0.xml', category: '📰 Top-Level World & US News' },

  // --- 🌍 International & Regional News ---
  { name: 'Deutsche Welle (DW) - All', url: 'https://rss.dw.com/rdf/rss-en-all', category: '🌍 International & Regional News' },
  { name: 'Le Monde - International (English)', url: 'https://www.lemonde.fr/en/international/rss_full.xml', category: '🌍 International & Regional News' },
  { name: 'Times of India - Top Stories', url: 'https://timesofindia.indiatimes.com/rssfeedstopstories.cms', category: '🌍 International & Regional News' },
  { name: 'BBC News - Asia', url: 'http://feeds.bbci.co.uk/news/world/asia/rss.xml', category: '🌍 International & Regional News' },
  { name: 'BBC News - Europe', url: 'http://feeds.bbci.co.uk/news/world/europe/rss.xml', category: '🌍 International & Regional News' },
  { name: 'Axios - World', url: 'https://api.axios.com/feed/world', category: '🌍 International & Regional News' },
  { name: 'Foreign Policy', url: 'https://foreignpolicy.com/feed/', category: '🌍 International & Regional News' },

  // --- 🏛️ Politics & Policy ---
  { name: 'Politico', url: 'https://rss.politico.com/politics-news.xml', category: '🏛️ Politics & Policy' },
  { name: 'The Hill', url: 'https://thehill.com/rss/syndicator/19109', category: '🏛️ Politics & Policy' },
  { name: 'NPR - Politics', url: 'https://feeds.npr.org/1014/rss.xml', category: '🏛️ Politics & Policy' },
  { name: 'New York Times - Politics', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Politics.xml', category: '🏛️ Politics & Policy' },

  // --- 💼 Business & Finance ---
  { name: 'Wall Street Journal - Business', url: 'https://feeds.a.dj.com/rss/WSJcomUSBusiness.xml', category: '💼 Business & Finance' },
  { name: 'The Economist - Business', url: 'https://www.economist.com/business/rss.xml', category: '💼 Business & Finance' },
  { name: 'Financial Times - Home (UK)', url: 'https://www.ft.com/rss/home/uk', category: '💼 Business & Finance' },
  { name: 'Bloomberg - Top News', url: 'https://feeds.bloomberg.com/windows/rss.xml', category: '💼 Business & Finance' },
  { name: 'CNBC - Top News', url: 'https://www.cnbc.com/id/100003114/device/rss/rss.html', category: '💼 Business & Finance' },
  { name: 'Harvard Business Review', url: 'https://hbr.org/feed', category: '💼 Business & Finance' },
  { name: 'Forbes', url: 'https://www.forbes.com/rss/', category: '💼 Business & Finance' },
  { name: 'MarketWatch - Top Stories', url: 'http://feeds.marketwatch.com/marketwatch/topstories/', category: '💼 Business & Finance' },

  // --- 💻 Technology (General) ---
  { name: 'TechCrunch', url: 'https://techcrunch.com/feed/', category: '💻 Technology (General)' },
  { name: 'The Verge', url: 'https://www.theverge.com/rss/index.xml', category: '💻 Technology (General)' },
  { name: 'Ars Technica', url: 'http://feeds.arstechnica.com/arstechnica/index', category: '💻 Technology (General)' },
  { name: 'Wired', url: 'https://www.wired.com/feed/rss', category: '💻 Technology (General)' },
  { name: 'Hacker News', url: 'https://news.ycombinator.com/rss', category: '💻 Technology (General)' },
  { name: 'ZDNet', url: 'https://www.zdnet.com/feed/', category: '💻 Technology (General)' },
  { name: 'Engadget', url: 'https://www.engadget.com/rss.xml', category: '💻 Technology (General)' },
  { name: 'MIT Technology Review', url: 'https://www.technologyreview.com/feed/', category: '💻 Technology (General)' },
  { name: 'Techmeme', url: 'https://www.techmeme.com/feed.xml', category: '💻 Technology (General)' },

  // --- 🤖 AI & Cybersecurity ---
  { name: 'Google AI Blog', url: 'https://research.google/blog/rss/', category: '🤖 AI & Cybersecurity' },
  { name: 'The Hacker News', url: 'http://feeds.feedburner.com/TheHackersNews', category: '🤖 AI & Cybersecurity' },
  { name: 'Krebs on Security', url: 'https://krebsonsecurity.com/feed/', category: '🤖 AI & Cybersecurity' },
  { name: 'Schneier on Security', url: 'https://www.schneier.com/feed/atom/', category: '🤖 AI & Cybersecurity' },
  { name: 'Dark Reading', url: 'https://www.darkreading.com/rss_simple.asp', category: '🤖 AI & Cybersecurity' },
  { name: 'Bleeping Computer', url: 'https://www.bleepingcomputer.com/feed/', category: '🤖 AI & Cybersecurity' },

  // --- 🔬 Science & Space ---
  { name: 'NASA - Breaking News', url: 'https://www.nasa.gov/rss/dyn/breaking_news.rss', category: '🔬 Science & Space' },
  { name: 'Nature', url: 'https://www.nature.com/nature.rss', category: '🔬 Science & Space' },
  { name: 'Scientific American', url: 'https://www.scientificamerican.com/feed/rss.cfm', category: '🔬 Science & Space' },
  { name: 'ScienceDaily', url: 'http://feeds.sciencedaily.com/sciencedaily', category: '🔬 Science & Space' },
  { name: 'Quanta Magazine', url: 'https://api.quantamagazine.org/feed/', category: '🔬 Science & Space' },
  { name: 'Space.com', url: 'https://www.space.com/feeds/all', category: '🔬 Science & Space' },

  // --- 🩺 Health & Medicine ---
  { name: 'STAT News', url: 'https://www.statnews.com/feed/', category: '🩺 Health & Medicine' },
  { name: 'MedPage Today', url: 'https://www.medpagetoday.com/rss/headlines.xml', category: '🩺 Health & Medicine' },
  { name: 'NPR - Health', url: 'https://feeds.npr.org/1007/rss.xml', category: '🩺 Health & Medicine' },
  { name: 'New York Times - Health', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Health.xml', category: '🩺 Health & Medicine' },

  // --- 🌿 Environment & Climate ---
  { name: 'Grist', url: 'https://grist.org/feed/', category: '🌿 Environment & Climate' },
  { name: 'Inside Climate News', url: 'https://insideclimatenews.org/feed/', category: '🌿 Environment & Climate' },
  { name: 'The Guardian - Environment', url: 'https://www.theguardian.com/environment/rss', category: '🌿 Environment & Climate' },

  // --- 🤔 Culture, Opinion & Long-form ---
  { name: 'The Atlantic', url: 'https://www.theatlantic.com/feed/all/', category: '🤔 Culture, Opinion & Long-form' },
  { name: 'The New Yorker', url: 'https://www.newyorker.com/feed/everything', category: '🤔 Culture, Opinion & Long-form' },
  { name: 'Aeon', url: 'https://aeon.co/feed.rss', category: '🤔 Culture, Opinion & Long-form' },
  { name: 'NYT - Opinion', url: 'https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml', category: '🤔 Culture, Opinion & Long-form' },
  { name: 'The Guardian - Opinion', url: 'https://www.theguardian.com/commentisfree/rss', category: '🤔 Culture, Opinion & Long-form' },

  // --- 🎬 Entertainment & Fandom ---
  { name: 'Variety', url: 'https://variety.com/feed/', category: '🎬 Entertainment & Fandom' },
  { name: 'The Hollywood Reporter', url: 'https://www.hollywoodreporter.com/feed/', category: '🎬 Entertainment & Fandom' },
  { name: 'Deadline', url: 'https://deadline.com/feed/', category: '🎬 Entertainment & Fandom' },
  { name: 'Comic Book Resources (CBR)', url: 'https://www.cbr.com/feed/', category: '🎬 Entertainment & Fandom' },
  { name: 'The Mary Sue', url: 'https://www.themarysue.com/feed/', category: '🎬 Entertainment & Fandom' },

  // --- 🎨 Creative: Art & Photography ---
  { name: 'Colossal (Art)', url: 'http://feeds.feedburner.com/colossal', category: '🎨 Creative: Art & Photography' },
  { name: 'Hyperallergic', url: 'https://hyperallergic.com/feed/', category: '🎨 Creative: Art & Photography' },
  { name: 'PetaPixel (Photography)', url: 'https://petapixel.com/feed/', category: '🎨 Creative: Art & Photography' },
  { name: 'Booooooom', url: 'https://www.booooooom.com/feed/', category: '🎨 Creative: Art & Photography' },

  // --- 🏠 Creative: Design & Architecture ---
  { name: 'Dezeen', url: 'http://feeds.feedburner.com/dezeen', category: '🏠 Creative: Design & Architecture' },
  { name: 'designboom', url: 'https://www.designboom.com/feed/', category: '🏠 Creative: Design & Architecture' },
  { name: 'Swissmiss', url: 'https://www.swiss-miss.com/feed', category: '🏠 Creative: Design & Architecture' },
  { name: 'Curbed', url: 'https://www.curbed.com/rss/index.xml', category: '🏠 Creative: Design & Architecture' },
  
  // --- 👟 Creative: Fashion & Style ---
  { name: 'Vogue', url: 'https://www.vogue.com/feed/rss', category: '👟 Creative: Fashion & Style' },
  { name: 'Hypebeast', url: 'https://hypebeast.com/feed', category: '👟 Creative: Fashion & Style' },

  // --- 🍔 Hobbies: Food & Cooking ---
  { name: 'Eater (All)', url: 'https://www.eater.com/rss/index.xml', category: '🍔 Hobbies: Food & Cooking' },
  { name: 'Bon Appétit', url: 'https://www.bonappetit.com/feed/rss', category: '🍔 Hobbies: Food & Cooking' },
  { name: 'Smitten Kitchen', url: 'http://feeds.feedburner.com/SmittenKitchen', category: '🍔 Hobbies: Food & Cooking' },

  // --- 🎮 Hobbies: Video Games ---
  { name: 'Kotaku', url: 'https://kotaku.com/rss', category: '🎮 Hobbies: Video Games' },
  { name: 'IGN', url: 'http://feeds.ign.com/ign/all', category: '🎮 Hobbies: Video Games' },
  { name: 'Game Rant', url: 'https://gamerant.com/feed/', category: '🎮 Hobbies: Video Games' },
  { name: 'GameSpot - All News', url: 'https://www.gamespot.com/feeds/news/', category: '🎮 Hobbies: Video Games' },
  { name: 'Polygon', url: 'https://www.polygon.com/rss/index.xml', category: '🎮 Hobbies: Video Games' },

  // --- ⚽ Hobbies: Sports ---
  { name: 'ESPN', url: 'https://www.espn.com/espn/rss/news', category: '⚽ Hobbies: Sports' },
  { name: 'BBC Sport', url: 'http://feeds.bbci.co.uk/sport/rss.xml', category: '⚽ Hobbies: Sports' },
  { name: 'SB Nation', url: 'httpss://www.sbnation.com/rss/index.xml', category: '⚽ Hobbies: Sports' },

  // --- 🚗 Hobbies: Automotive ---
  { name: 'Jalopnik', url: 'https://jalopnik.com/rss', category: '🚗 Hobbies: Automotive' },

  // --- 📚 Intellectual: History, Philosophy, Literature ---
  { name: 'Daily Stoic', url: 'https://dailystoic.com/feed/', category: '📚 Intellectual: History, Philosophy, Literature' },

  // --- 💡 Lifestyle & Productivity ---
  { name: 'Lifehacker', url: 'https://lifehacker.com/rss', category: '💡 Lifestyle & Productivity' },
  { name: 'Fast Company', url: 'https://www.fastcompany.com/rss', category: '💡 Lifestyle & Productivity' },
  { name: 'WIRED - Ideas', url: 'httpsS://www.wired.com/feed/category/ideas/latest/rss', category: '💡 Lifestyle & Productivity' },

  // --- 🕵️ Investigative & Fact-Checking ---
  { name: 'ProPublica', url: 'http://feeds.propublica.org/propublica/main', category: '🕵️ Investigative & Fact-Checking' },
  { name: 'Snopes', url: 'https://www.snopes.com/feed/', category: '🕵️ Investigative & Fact-Checking' },
  { name: 'The Intercept', url: 'https://theintercept.com/feed/?lang=en', category: '🕵️ Investigative & Fact-Checking' },

  // --- 🎙️ Popular Podcasts (as Feeds) ---
  { name: '99% Invisible', url: 'http://feeds.99percentinvisible.org/99percentinvisible', category: '🎙️ Popular Podcasts (as Feeds)' },
  { name: 'This American Life', url: 'http://feeds.thisamericanlife.org/talpodcast', category: '🎙️ Popular Podcasts (as Feeds)' },
  { name: 'Radiolab', url: 'http://feeds.wnyc.org/radiolab', category: '🎙️ Popular Podcasts (as Feeds)' },
  { name: 'Freakonomics Radio', url: 'http://feeds.feedburner.com/freakonomicsradio', category: '🎙️ Popular Podcasts (as Feeds)' },
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
                    category: feed.category, // <-- Use the new, specific category
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