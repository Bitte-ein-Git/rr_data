import fs from 'fs-extra';
import path from 'path';

// Config
const OWNER = 'impactcoding';
const REPO = 'rr-player-database';
const FILE_PATH = 'rr-players.json';
const ARCHIVE_DIR = 'archive';
const DAYS_BACK = 30;
const CONCURRENCY = 20;

// Token from Env (Required for API Limits)
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

const headers = {
    'Accept': 'application/vnd.github.v3+json',
    ...(GITHUB_TOKEN ? { 'Authorization': `Bearer ${GITHUB_TOKEN}` } : {})
};

async function run() {
    try {
        console.log(`Starting API backfill for last ${DAYS_BACK} days...`);
        await fs.ensureDir(ARCHIVE_DIR);
        await fs.emptyDir(ARCHIVE_DIR); // Clean start

        const since = new Date(Date.now() - DAYS_BACK * 24 * 60 * 60 * 1000).toISOString();
        
        // 1. Fetch Commits
        let commits = [];
        let page = 1;
        while (true) {
            const url = `https://api.github.com/repos/${OWNER}/${REPO}/commits?path=${FILE_PATH}&since=${since}&per_page=100&page=${page}`;
            const res = await fetch(url, { headers });
            if (!res.ok) throw new Error(`Failed to fetch commits: ${res.status} ${res.statusText}`);
            
            const data = await res.json();
            if (data.length === 0) break;
            
            commits = commits.concat(data);
            process.stdout.write(`\rFetched ${commits.length} commits...`);
            page++;
        }
        console.log(`\nTotal commits to process: ${commits.length}`);

        // 2. Fetch File Contents (Concurrent) & Parse
        // Store temporarily as { date: string, players: Array }
        const snapshots = [];
        
        let processedCount = 0;
        const queue = [...commits];

        async function worker() {
            while (queue.length > 0) {
                const commit = queue.shift();
                try {
                    const url = `https://api.github.com/repos/${OWNER}/${REPO}/contents/${FILE_PATH}?ref=${commit.sha}`;
                    const res = await fetch(url, { headers });
                    
                    if (res.ok) {
                        const d = await res.json();
                        const content = Buffer.from(d.content, "base64").toString("utf8");
                        const json = JSON.parse(content);
                        
                        snapshots.push({
                            date: commit.commit.author.date,
                            players: json
                        });
                    }
                } catch (e) {
                    console.warn(`\nFailed to fetch/parse ${commit.sha}: ${e.message}`);
                }
                
                processedCount++;
                if (processedCount % 10 === 0) process.stdout.write(`\rProcessing contents: ${processedCount}/${commits.length}`);
            }
        }

        const workers = Array(CONCURRENCY).fill(null).map(worker);
        await Promise.all(workers);
        console.log('\nDownloads complete. Building history...');

        // 3. Sort snapshots by date (oldest first)
        snapshots.sort((a, b) => new Date(a.date) - new Date(b.date));

        // 4. Build Player History
        const playerMap = new Map();

        snapshots.forEach(snapshot => {
            snapshot.players.forEach(p => {
                if (!p.fc) return;

                if (!playerMap.has(p.fc)) {
                    playerMap.set(p.fc, {
                        name: p.name || 'Unknown',
                        fc: p.fc,
                        vr_history: []
                    });
                }

                const entry = playerMap.get(p.fc);
                entry.name = p.name || entry.name; // Keep newest name

                const currentVR = parseInt(p.ev || p.vr || 0);
                // Calculate delta
                const lastHistory = entry.vr_history[entry.vr_history.length - 1];
                const prevVR = lastHistory ? lastHistory.totalVR : currentVR;

                entry.vr_history.push({
                    date: snapshot.date,
                    vrChange: currentVR - prevVR,
                    totalVR: currentVR
                });
            });
        });

        // 5. Write Archive Files
        console.log('Writing individual player files...');
        for (const [fc, data] of playerMap) {
            const safeFC = fc.replace(/[^a-zA-Z0-9-]/g, '_');
            await fs.writeJson(path.join(ARCHIVE_DIR, `${safeFC}.json`), data, { spaces: 2 });
        }

        console.log('Backfill Done.');

    } catch (e) {
        console.error(e);
        process.exit(1);
    }
}

run();