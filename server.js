const express = require('express');
const mysql = require('mysql2/promise');
const { exec } = require('child_process');
const util = require('util');
const execPromise = util.promisify(exec);
const axios = require('axios');
const cors = require('cors');
const bodyParser = require('body-parser');
const cron = require('node-cron');
const fs = require('fs').promises;
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static('public'));
app.use('/js', express.static(path.join(__dirname, 'JS')));

// View engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'ejs');
app.engine('html', require('ejs').renderFile);

// Configuration
const config = {
    solr: {
        host: process.env.SOLR_HOST || 'localhost',
        port: process.env.SOLR_PORT || 8983,
        core: process.env.SOLR_CORE || 'crawler'
    },
    storm: {
        nimbusHost: process.env.NIMBUS_HOST || 'nimbus',
        uiHost: process.env.STORM_UI_HOST || 'localhost',
        uiPort: process.env.STORM_UI_PORT || 8080
    },
    frontier: {
        host: process.env.FRONTIER_HOST || 'localhost',
        port: process.env.FRONTIER_PORT || 7071
    },
    mysql: {
        host: process.env.MYSQL_HOST || 'localhost',
        user: process.env.MYSQL_USER || 'root',
        password: process.env.MYSQL_PASSWORD || '',
        database: process.env.MYSQL_DB || 'stormcrawler_manager'
    }
};

// In-memory storage for jobs (simplified version without MySQL dependency)
const jobs = [];
const seedUrls = new Map();
const crawlStats = new Map();
let jobIdCounter = 1;

// Crawl Manager Class
class CrawlManager {
    constructor() {
        this.activeJobs = new Map();
        this.scheduledJobs = new Map();
    }

    async createJob(jobData) {
        const connection = await dbPool.getConnection();
        try {
            const [result] = await connection.execute(
                `INSERT INTO crawl_jobs 
                (name, max_depth, max_time_minutes, politeness_delay, max_urls_per_host, auto_mode, schedule_cron) 
                VALUES (?, ?, ?, ?, ?, ?, ?)`,
                [
                    jobData.name,
                    jobData.maxDepth || 3,
                    jobData.maxTimeMinutes || 60,
                    jobData.politenessDelay || 1000,
                    jobData.maxUrlsPerHost || 100,
                    jobData.autoMode || false,
                    jobData.scheduleCron || null
                ]
            );

            const jobId = result.insertId;

            // Add seed URLs
            if (jobData.seedUrls && jobData.seedUrls.length > 0) {
                for (const url of jobData.seedUrls) {
                    await connection.execute(
                        'INSERT INTO seed_urls (job_id, url) VALUES (?, ?)',
                        [jobId, url]
                    );
                }
            }

            // Schedule if auto mode is enabled
            if (jobData.autoMode && jobData.scheduleCron) {
                this.scheduleJob(jobId, jobData.scheduleCron);
            }

            return jobId;
        } finally {
            connection.release();
        }
    }

    async startJob(jobId) {
        const connection = await dbPool.getConnection();
        try {
            // Get job details
            const [jobs] = await connection.execute(
                'SELECT * FROM crawl_jobs WHERE id = ?',
                [jobId]
            );

            if (jobs.length === 0) {
                throw new Error('Job not found');
            }

            const job = jobs[0];

            // Get seed URLs
            const [seedUrls] = await connection.execute(
                'SELECT url FROM seed_urls WHERE job_id = ?',
                [jobId]
            );

            // Create topology configuration
            const topologyConfig = await this.createTopologyConfig(job, seedUrls);

            // Submit topology to Storm
            const topologyId = await this.submitTopology(job.name, topologyConfig);

            // Update job status
            await connection.execute(
                'UPDATE crawl_jobs SET status = ?, topology_id = ?, started_at = NOW() WHERE id = ?',
                ['running', topologyId, jobId]
            );

            // Start monitoring
            this.startMonitoring(jobId, topologyId);

            return topologyId;
        } finally {
            connection.release();
        }
    }

    async stopJob(jobId) {
        const connection = await dbPool.getConnection();
        try {
            const [jobs] = await connection.execute(
                'SELECT topology_id FROM crawl_jobs WHERE id = ?',
                [jobId]
            );

            if (jobs.length > 0 && jobs[0].topology_id) {
                // Kill topology
                await this.killTopology(jobs[0].topology_id);

                // Update status
                await connection.execute(
                    'UPDATE crawl_jobs SET status = ?, completed_at = NOW() WHERE id = ?',
                    ['stopped', jobId]
                );
            }

            // Stop monitoring
            if (this.activeJobs.has(jobId)) {
                clearInterval(this.activeJobs.get(jobId));
                this.activeJobs.delete(jobId);
            }
        } finally {
            connection.release();
        }
    }

    async createTopologyConfig(job, seedUrls) {
        const config = {
            'topology.name': job.name,
            'topology.max.spout.pending': 100,
            'topology.debug': false,
            'fetcher.threads.per.queue': 1,
            'fetcher.max.urls.in.queues': 100,
            'http.content.limit': 1048576,
            'http.timeout': 10000,
            'http.protocol.implementation': 'com.digitalpebble.stormcrawler.protocol.httpclient.HttpProtocol',
            'partition.url.mode': 'byHost',
            'metadata.persist': ['fetch.statusCode', 'fetch.timestamp', 'parse.title'],
            'metadata.track.depth': true,
            'metadata.max.depth': job.max_depth,
            'fetcher.max.crawl.delay': job.politeness_delay,
            'fetcher.max.urls.per.host': job.max_urls_per_host,
            'frontier.host': config.frontier.host,
            'frontier.port': config.frontier.port,
            'solr.indexer.url': `http://${config.solr.host}:${config.solr.port}/solr/${config.solr.core}`,
            'seeds': seedUrls.map(s => s.url)
        };

        // Save config to file
        const configPath = `/tmp/topology-${job.id}.yaml`;
        await fs.writeFile(configPath, this.yamlStringify(config));

        return configPath;
    }

    async submitTopology(name, configPath) {
        try {
            const jarPath = '/crawldata/crawler/target/crawler-1.0.jar';
            const mainClass = 'com.digitalpebble.CrawlTopology';
            
            const command = `storm jar ${jarPath} ${mainClass} -conf ${configPath} -local`;
            
            const { stdout, stderr } = await execPromise(command, {
                cwd: '/crawldata/crawler'
            });

            // Extract topology ID from output
            const match = stdout.match(/Submitted topology (\S+)/);
            return match ? match[1] : `${name}-${Date.now()}`;
        } catch (error) {
            console.error('Failed to submit topology:', error);
            throw error;
        }
    }

    async killTopology(topologyId) {
        try {
            const command = `storm kill ${topologyId} -w 0`;
            await execPromise(command);
        } catch (error) {
            console.error('Failed to kill topology:', error);
        }
    }

    startMonitoring(jobId, topologyId) {
        const interval = setInterval(async () => {
            try {
                // Get metrics from Storm UI
                const metrics = await this.getTopologyMetrics(topologyId);
                
                // Store metrics in database
                const connection = await dbPool.getConnection();
                try {
                    await connection.execute(
                        `INSERT INTO crawl_stats 
                        (job_id, urls_fetched, urls_failed, bytes_downloaded, avg_fetch_time) 
                        VALUES (?, ?, ?, ?, ?)`,
                        [jobId, metrics.fetched, metrics.failed, metrics.bytes, metrics.avgTime]
                    );

                    // Update job stats
                    await connection.execute(
                        `UPDATE crawl_jobs 
                        SET urls_crawled = ?, urls_discovered = ? 
                        WHERE id = ?`,
                        [metrics.fetched, metrics.discovered, jobId]
                    );

                    // Check if job should be stopped (max time reached)
                    const [jobs] = await connection.execute(
                        'SELECT started_at, max_time_minutes FROM crawl_jobs WHERE id = ?',
                        [jobId]
                    );

                    if (jobs.length > 0) {
                        const job = jobs[0];
                        const elapsedMinutes = (Date.now() - new Date(job.started_at)) / 60000;
                        
                        if (elapsedMinutes >= job.max_time_minutes) {
                            await this.stopJob(jobId);
                        }
                    }
                } finally {
                    connection.release();
                }
            } catch (error) {
                console.error('Monitoring error:', error);
            }
        }, 30000); // Check every 30 seconds

        this.activeJobs.set(jobId, interval);
    }

    async getTopologyMetrics(topologyId) {
        try {
            const response = await axios.get(
                `http://${config.storm.uiHost}:${config.storm.uiPort}/api/v1/topology/${topologyId}`
            );

            const data = response.data;
            
            // Extract metrics from Storm UI response
            return {
                fetched: data.spouts?.[0]?.emitted || 0,
                failed: data.spouts?.[0]?.failed || 0,
                discovered: data.bolts?.[0]?.emitted || 0,
                bytes: 0, // Would need custom metrics
                avgTime: data.spouts?.[0]?.completeLatency || 0
            };
        } catch (error) {
            console.error('Failed to get topology metrics:', error);
            return {
                fetched: 0,
                failed: 0,
                discovered: 0,
                bytes: 0,
                avgTime: 0
            };
        }
    }

    scheduleJob(jobId, cronExpression) {
        if (this.scheduledJobs.has(jobId)) {
            this.scheduledJobs.get(jobId).stop();
        }

        const task = cron.schedule(cronExpression, async () => {
            console.log(`Running scheduled job ${jobId}`);
            try {
                await this.startJob(jobId);
            } catch (error) {
                console.error(`Failed to start scheduled job ${jobId}:`, error);
            }
        });

        this.scheduledJobs.set(jobId, task);
        task.start();
    }

    yamlStringify(obj) {
        let yaml = '';
        for (const [key, value] of Object.entries(obj)) {
            if (Array.isArray(value)) {
                yaml += `${key}:\n`;
                value.forEach(item => {
                    yaml += `  - ${item}\n`;
                });
            } else {
                yaml += `${key}: ${value}\n`;
            }
        }
        return yaml;
    }
}

const crawlManager = new CrawlManager();

// API Routes

// Search endpoints
app.get('/api/search', async (req, res) => {
    try {
        const query = req.query.q || '*:*';
        const start = parseInt(req.query.start) || 0;
        const rows = parseInt(req.query.rows) || 10;

        const solrUrl = `http://${config.solr.host}:${config.solr.port}/solr/${config.solr.core}/select`;
        
        const response = await axios.get(solrUrl, {
            params: {
                q: query,
                start: start,
                rows: rows,
                wt: 'json',
                defType: 'edismax',
                qf: 'title^2 text keywords_t description',
                hl: 'true',
                'hl.fl': 'title,text',
                'hl.simple.pre': '<mark>',
                'hl.simple.post': '</mark>'
            }
        });

        res.json(response.data);
    } catch (error) {
        console.error('Search error:', error);
        res.status(500).json({ error: 'Search failed' });
    }
});

app.get('/api/suggest', async (req, res) => {
    try {
        const query = req.query.q || '';
        
        const solrUrl = `http://${config.solr.host}:${config.solr.port}/solr/${config.solr.core}/suggest`;
        
        const response = await axios.get(solrUrl, {
            params: {
                q: query,
                wt: 'json'
            }
        });

        res.json(response.data);
    } catch (error) {
        console.error('Suggest error:', error);
        res.status(500).json({ error: 'Suggest failed' });
    }
});

// Crawl management endpoints
app.get('/api/jobs', async (req, res) => {
    try {
        const [jobs] = await dbPool.execute(
            'SELECT * FROM crawl_jobs ORDER BY created_at DESC'
        );
        res.json(jobs);
    } catch (error) {
        console.error('Error fetching jobs:', error);
        res.status(500).json({ error: 'Failed to fetch jobs' });
    }
});

app.get('/api/jobs/:id', async (req, res) => {
    try {
        const [jobs] = await dbPool.execute(
            'SELECT * FROM crawl_jobs WHERE id = ?',
            [req.params.id]
        );

        if (jobs.length === 0) {
            return res.status(404).json({ error: 'Job not found' });
        }

        const [seedUrls] = await dbPool.execute(
            'SELECT * FROM seed_urls WHERE job_id = ?',
            [req.params.id]
        );

        const [stats] = await dbPool.execute(
            'SELECT * FROM crawl_stats WHERE job_id = ? ORDER BY timestamp DESC LIMIT 20',
            [req.params.id]
        );

        res.json({
            job: jobs[0],
            seedUrls: seedUrls,
            stats: stats
        });
    } catch (error) {
        console.error('Error fetching job:', error);
        res.status(500).json({ error: 'Failed to fetch job' });
    }
});

app.post('/api/jobs', async (req, res) => {
    try {
        const jobId = await crawlManager.createJob(req.body);
        res.json({ id: jobId, message: 'Job created successfully' });
    } catch (error) {
        console.error('Error creating job:', error);
        res.status(500).json({ error: 'Failed to create job' });
    }
});

app.post('/api/jobs/:id/start', async (req, res) => {
    try {
        const topologyId = await crawlManager.startJob(req.params.id);
        res.json({ topologyId: topologyId, message: 'Job started successfully' });
    } catch (error) {
        console.error('Error starting job:', error);
        res.status(500).json({ error: 'Failed to start job' });
    }
});

app.post('/api/jobs/:id/stop', async (req, res) => {
    try {
        await crawlManager.stopJob(req.params.id);
        res.json({ message: 'Job stopped successfully' });
    } catch (error) {
        console.error('Error stopping job:', error);
        res.status(500).json({ error: 'Failed to stop job' });
    }
});

app.delete('/api/jobs/:id', async (req, res) => {
    try {
        // Stop job if running
        await crawlManager.stopJob(req.params.id);
        
        // Delete from database
        await dbPool.execute('DELETE FROM crawl_jobs WHERE id = ?', [req.params.id]);
        
        res.json({ message: 'Job deleted successfully' });
    } catch (error) {
        console.error('Error deleting job:', error);
        res.status(500).json({ error: 'Failed to delete job' });
    }
});

// Frontier management
app.get('/api/frontier/stats', async (req, res) => {
    try {
        const response = await axios.get(
            `http://${config.frontier.host}:${config.frontier.port}/stats`
        );
        res.json(response.data);
    } catch (error) {
        console.error('Error fetching frontier stats:', error);
        res.status(500).json({ error: 'Failed to fetch frontier stats' });
    }
});

// Storm cluster status
app.get('/api/cluster/status', async (req, res) => {
    try {
        const response = await axios.get(
            `http://${config.storm.uiHost}:${config.storm.uiPort}/api/v1/cluster/summary`
        );
        res.json(response.data);
    } catch (error) {
        console.error('Error fetching cluster status:', error);
        res.status(500).json({ error: 'Failed to fetch cluster status' });
    }
});

// Main UI routes
app.get('/', (req, res) => {
    res.render('dashboard.html');
});

app.get('/search', (req, res) => {
    res.render('search.html');
});

app.get('/jobs', (req, res) => {
    res.render('jobs.html');
});

app.get('/jobs/:id', (req, res) => {
    res.render('job-detail.html');
});

// Initialize and start server
async function startServer() {
    await initDatabase();
    
    app.listen(PORT, () => {
        console.log(`StormCrawler Manager running on port ${PORT}`);
        console.log(`Dashboard: http://localhost:${PORT}`);
        console.log(`Search: http://localhost:${PORT}/search`);
        console.log(`Jobs: http://localhost:${PORT}/jobs`);
    });
}

startServer().catch(console.error);

module.exports = app;
