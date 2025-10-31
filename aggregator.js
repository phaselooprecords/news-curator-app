// aggregator.js (UPDATED: Now uses a Worker Thread)

const cron = require('cron');
const { Worker } = require('worker_threads');
const path = require('path');
const db = require('./database'); // Still needed for getNews

// --- CORE FUNCTION TO FETCH AND SAVE NEWS ---
// This function now just creates and manages a worker
function runFetchInWorker() {
    console.log('[Aggregator] Creating worker thread for news fetch...');
    
    // Create a new worker
    const worker = new Worker(path.resolve(__dirname, 'fetch-worker.js'));

    // Listen for messages from the worker
    worker.on('message', (message) => {
        if (message.status === 'done') {
            console.log(`[Aggregator] Worker finished, processed ${message.count} articles.`);
        } else if (message.status === 'error') {
            console.error(`[Aggregator] Worker encountered an error: ${message.error}`);
        }
    });

    // Listen for any errors from the worker itself
    worker.on('error', (error) => {
        console.error('[Aggregator] Worker thread error:', error);
    });

    // Listen for when the worker exits
    worker.on('exit', (code) => {
        if (code !== 0) {
            console.error(`[Aggregator] Worker stopped with exit code ${code}`);
        }
    });
}

// --- CRON JOB SETUP ---
const NEWS_CRON_PATTERN = '0 */2 * * *'; // Runs every 2 hours

// The cron job now calls the worker function
const newsJob = new cron.CronJob(NEWS_CRON_PATTERN, runFetchInWorker, null, false, 'UTC');

// --- EXPORTS ---
module.exports = {
    startScheduler: () => {
        newsJob.start();
        console.log(`[Scheduler] RSS job scheduled on pattern: ${NEWS_CRON_PATTERN}`);
        
        // OPTIONAL: You can trigger one initial run
        // This will now be safe as it runs in the background
        console.log("[Scheduler] Triggering initial background fetch...");
        runFetchInWorker();
    },
    runFetch: runFetchInWorker, // Export the worker-based function
    getNews: db.getAllArticles // This remains the same
};