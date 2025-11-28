import { execSync, spawn } from 'child_process';
import fs from 'fs-extra';
import path from 'path';
import pLimit from 'p-limit';

// config
const REPO_URL = 'https://github.com/impactcoding/rr-player-database.git';
const TEMP_GIT_DIR = 'temp_rr_db.git'; // bare repo folder
const ARCHIVE_DIR = 'archive';
const FILE_PATH = 'rr-players.json';
const DAYS_BACK = 30;
const PARALLEL_LIMIT = 50; // number of concurrent git processes

async function run() {
    try {
        console.log('Cleaning up...');
        await fs.remove(TEMP_GIT_DIR);
        await fs.remove(ARCHIVE_DIR);
        await fs.ensureDir(ARCHIVE_DIR);

        // 1. Bare Clone (Fast, no working tree)
        console.log('Cloning bare repository...');
        execSync(`git clone --bare ${REPO_URL} ${TEMP_GIT_DIR}`, { stdio: 'inherit' });

        // 2. Get Commit Hashes & Dates
        console.log('Fetching commit list...');
        const sinceDate = new Date(Date.now() - DAYS_BACK * 24 * 60 * 60 * 1000).toISOString();
        
        // git log format: HASH|ISO_DATE
        const logCmd = `git --git-dir=${TEMP_GIT_DIR} log --since="${sinceDate}" --pretty=format:"%H|%aI" -- ${FILE_PATH}`;
        const logOutput = execSync(logCmd, { maxBuffer: 1024 * 1024 * 10 }).toString();
        
        const commits = logOutput.split('\n')
            .filter(l => l.trim())
            .map(line => {
                const [hash, date] = line.split('|');
                return { hash, date };
            })
            .reverse(); // Process oldest to newest

        console.log(`Found ${commits.length} commits. Processing with concurrency ${PARALLEL_LIMIT}...`);

        // 3. Process Commits in Parallel
        const limit = pLimit(PARALLEL_LIMIT);
        const snapshotMap = new Map(); // key: timestamp, val: players[]

        const tasks = commits.map((commit, idx) => limit(async () => {
            try {
                // Spawn git show to read file content directly from packfile
                const content = await getFileContent(commit.hash);
                const players = JSON.parse(content);
                
                // Store in memory (timestamp key ensures order later)
                snapshotMap.set(commit.date, players);
                
                if (idx % 500 === 0) process.stdout.write(`\rProcessed: ${idx}/${commits.length}`);
            } catch (e) {
                // Ignore corrupt/empty JSONs in history
            }
        }));

        await Promise.all(tasks);
        console.log('\nData extraction complete. Building player history...');

        // 4. Sort and Build Per-Player History
        // Sort keys to ensure chronological order
        const sortedDates = Array.from(snapshotMap.keys()).sort((a, b) => new Date(a) - new Date(b));
        const playerMap = new Map();

        for (const date of sortedDates) {
            const players = snapshotMap.get(date);
            
            for (const p of players) {
                if (!p.fc) continue;

                if (!playerMap.has(p.fc)) {
                    playerMap.set(p.fc, {
                        name: p.name || 'Unknown',
                        fc: p.fc,
                        vr_history: []
                    });
                }

                const entry = playerMap.get(p.fc);
                entry.name = p.name || entry.name;

                const currentVR = parseInt(p.ev || p.vr || 0);
                const lastHistory = entry.vr_history[entry.vr_history.length - 1];
                const prevVR = lastHistory ? lastHistory.totalVR : currentVR;

                // Push every single update (high resolution)
                entry.vr_history.push({
                    date: date,
                    vrChange: currentVR - prevVR,
                    totalVR: currentVR
                });
            }
        }

        // 5. Write Files
        console.log(`Writing ${playerMap.size} player files...`);
        for (const [fc, data] of playerMap) {
            const safeFC = fc.replace(/[^a-zA-Z0-9-]/g, '_');
            await fs.writeJson(path.join(ARCHIVE_DIR, `${safeFC}.json`), data, { spaces: 2 });
        }

        // Cleanup
        await fs.remove(TEMP_GIT_DIR);
        console.log('Backfill Done.');

    } catch (e) {
        console.error('\nFATAL:', e);
        process.exit(1);
    }
}

// Helper: Promisified git show
function getFileContent(commitHash) {
    return new Promise((resolve, reject) => {
        const child = spawn('git', ['--git-dir', TEMP_GIT_DIR, 'show', `${commitHash}:${FILE_PATH}`]);
        
        let data = '';
        child.stdout.on('data', chunk => data += chunk);
        
        child.on('close', code => {
            if (code === 0) resolve(data);
            else reject(new Error(`Git show failed for ${commitHash}`));
        });
        
        child.on('error', err => reject(err));
    });
}

run();