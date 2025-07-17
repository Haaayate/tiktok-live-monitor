// server.js - TikTokライブ監視バックエンド（PostgreSQL + CSV + 代替ライブラリ完全版）
const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const { WebcastPushConnection } = require('tiktok-live-connector');
const cors = require('cors');
const multer = require('multer');
const csv = require('csv-parser');
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');
const axios = require('axios');
const cheerio = require('cheerio');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST"]
  }
});

// PostgreSQL接続設定
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// ミドルウェア設定
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// ファイルアップロード設定
const upload = multer({ dest: 'uploads/' });

// 接続管理設定
const MAX_CONCURRENT_CONNECTIONS = 25; // 最大同時接続数を25に設定
const CONNECTION_RETRY_DELAY = 5000; // 接続リトライ間隔

// データストレージ（メモリ + DB）
let connections = new Map();
let liveData = new Map();
let connectionQueue = []; // 接続待機キュー

// データベース初期化
async function initializeDatabase() {
  try {
    console.log('データベース初期化開始...');
    
    // テーブル作成
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) UNIQUE NOT NULL,
        added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(50) DEFAULT 'monitoring',
        total_diamonds INTEGER DEFAULT 0,
        total_gifts INTEGER DEFAULT 0,
        total_comments INTEGER DEFAULT 0,
        last_live_check TIMESTAMP,
        is_live BOOLEAN DEFAULT false,
        viewer_count INTEGER DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // ライブデータ履歴テーブル
    await pool.query(`
      CREATE TABLE IF NOT EXISTS live_history (
        id SERIAL PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        diamonds INTEGER DEFAULT 0,
        gifts INTEGER DEFAULT 0,
        comments INTEGER DEFAULT 0,
        viewers INTEGER DEFAULT 0,
        is_live BOOLEAN DEFAULT false,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    `);
    
    // インデックス作成
    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
      CREATE INDEX IF NOT EXISTS idx_live_history_username ON live_history(username);
      CREATE INDEX IF NOT EXISTS idx_live_history_recorded_at ON live_history(recorded_at);
    `);
    
    console.log('データベース初期化完了');
    
    // 既存ユーザーの復元
    await restoreExistingUsers();
    
  } catch (error) {
    console.error('データベース初期化エラー:', error);
  }
}

// 既存ユーザーの復元
async function restoreExistingUsers() {
  try {
    console.log('既存ユーザーの復元開始...');
    
    const result = await pool.query(`
      SELECT username, total_diamonds, total_gifts, total_comments, 
             is_live, viewer_count, last_live_check, status
      FROM users 
      WHERE status IN ('monitoring', 'waiting')
      ORDER BY added_at ASC
    `);
    
    console.log(`${result.rows.length}件のユーザーを復元中...`);
    
    for (const user of result.rows) {
      console.log(`${user.username}: 復元開始 (status: ${user.status})`);
      
      // liveDataに復元（必ず作成）
      const userData = {
        username: user.username,
        isLive: user.is_live || false,
        viewerCount: user.viewer_count || 0,
        totalComments: user.total_comments || 0,
        totalGifts: user.total_gifts || 0,
        totalDiamonds: user.total_diamonds || 0,
        lastUpdate: user.last_live_check || new Date().toISOString(),
        recentComments: [],
        recentGifts: []
      };
      
      liveData.set(user.username, userData);
      console.log(`${user.username}: liveDataに復元完了`);
      
      // 接続数制限チェック
      if (user.status === 'monitoring' && connections.size < MAX_CONCURRENT_CONNECTIONS) {
        try {
          await connectToTikTokLive(user.username);
          console.log(`${user.username}: TikTok接続復元成功`);
        } catch (error) {
          console.log(`${user.username}: TikTok接続復元失敗 - ${error.message}`);
          // 接続失敗してもliveDataは保持
          // 接続失敗時は待機キューに追加
          connectionQueue.push(user.username);
          await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', user.username]);
        }
      } else {
        // 接続制限により待機キューに追加
        connectionQueue.push(user.username);
        await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', user.username]);
        console.log(`${user.username}: 接続制限により待機キューに追加`);
      }
    }
    
    console.log('既存ユーザーの復元完了');
    console.log(`liveData件数: ${liveData.size}`);
    console.log(`アクティブ接続: ${connections.size}`);
    console.log(`待機キュー: ${connectionQueue.length}`);
    
  } catch (error) {
    console.error('ユーザー復元エラー:', error);
  }
}

// 接続キュー処理
async function processConnectionQueue() {
  if (connectionQueue.length === 0) return;
  if (connections.size >= MAX_CONCURRENT_CONNECTIONS) return;
  
  const username = connectionQueue.shift();
  console.log(`キューから接続処理: ${username}`);
  
  try {
    await connectToTikTokLive(username);
    
    // データベースのステータスを更新
    await pool.query(`
      UPDATE users SET status = 'monitoring', updated_at = CURRENT_TIMESTAMP
      WHERE username = $1
    `, [username]);
    
    console.log(`${username}: キューからの接続成功`);
    
    // 通知送信
    io.emit('user-connected', { username, status: 'connected' });
    
    // 次のキューを処理
    setTimeout(() => {
      processConnectionQueue();
    }, CONNECTION_RETRY_DELAY);
    
  } catch (error) {
    console.error(`${username}: キューからの接続失敗`, error);
    
    // 失敗した場合は再度キューに追加
    connectionQueue.push(username);
  }
}

// 定期的なキュー処理
setInterval(() => {
  processConnectionQueue();
}, 30000); // 30秒ごと

// Socket.io接続管理
io.on('connection', (socket) => {
  console.log('クライアント接続:', socket.id);
  
  // 現在のライブデータを送信
  socket.emit('initial-data', Object.fromEntries(liveData));
  
  socket.on('disconnect', () => {
    console.log('クライアント切断:', socket.id);
  });
});

// 初期ユーザーデータ作成関数
function createInitialUserData(username) {
  return {
    username: username,
    isLive: true,
    viewerCount: 0,
    totalComments: 0,
    totalGifts: 0,
    totalDiamonds: 0,
    lastUpdate: new Date().toISOString(),
    recentComments: [],
    recentGifts: []
  };
}

// データベースにユーザーデータ保存
async function saveUserToDatabase(username, userData) {
  try {
    await pool.query(`
      INSERT INTO users (username, total_diamonds, total_gifts, total_comments, 
                        is_live, viewer_count, last_live_check)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      ON CONFLICT (username) 
      DO UPDATE SET 
        total_diamonds = $2,
        total_gifts = $3,
        total_comments = $4,
        is_live = $5,
        viewer_count = $6,
        last_live_check = $7,
        updated_at = CURRENT_TIMESTAMP
    `, [
      username,
      userData.totalDiamonds || 0,
      userData.totalGifts || 0,
      userData.totalComments || 0,
      userData.isLive || false,
      userData.viewerCount || 0,
      new Date()
    ]);
  } catch (error) {
    console.error(`${username}: データベース保存エラー`, error);
  }
}

// ライブ履歴データ保存
async function saveLiveHistory(username, userData) {
  try {
    await pool.query(`
      INSERT INTO live_history (username, diamonds, gifts, comments, viewers, is_live)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      username,
      userData.totalDiamonds || 0,
      userData.totalGifts || 0,
      userData.totalComments || 0,
      userData.viewerCount || 0,
      userData.isLive || false
    ]);
  } catch (error) {
    console.error(`${username}: 履歴保存エラー`, error);
  }
}

// =============================================================================
// 代替ライブラリによるライブ検出システム
// =============================================================================

// Web Scrapingによるライブ検出
async function checkLiveWithScraping(username) {
    console.log(`🕷️ [${username}] Web Scraping でライブ状態チェック開始`);
    
    try {
        const url = `https://www.tiktok.com/@${username}`;
        
        const response = await axios.get(url, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            },
            timeout: 15000
        });
        
        const $ = cheerio.load(response.data);
        
        // ライブ配信の指標を探す
        const indicators = {
            liveText: $('body').text().toLowerCase().includes('live'),
            liveClass: $('.live').length > 0 || $('[class*="live"]').length > 0,
            liveData: $('[data-live="true"]').length > 0,
            roomId: /room_id['"]\s*:\s*['"]\d+['"]/.test(response.data)
        };
        
        const isLive = Object.values(indicators).some(indicator => indicator);
        
        const result = {
            isLive: isLive,
            indicators: indicators,
            source: 'web-scraping'
        };
        
        console.log(`📊 [${username}] Web Scraping 結果:`, result);
        return result;
        
    } catch (error) {
        console.log(`❌ [${username}] Web Scraping エラー:`, error.message);
        throw error;
    }
}

// 直接APIによるライブ検出
async function checkLiveWithDirectAPI(username) {
    console.log(`🎯 [${username}] 直接API でライブ状態チェック開始`);
    
    try {
        const apiUrl = `https://www.tiktok.com/api/user/detail/?uniqueId=${username}`;
        
        const response = await axios.get(apiUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Referer': `https://www.tiktok.com/@${username}`,
                'Accept': 'application/json',
                'Accept-Language': 'en-US,en;q=0.9'
            },
            timeout: 10000
        });
        
        if (response.data && response.data.userInfo) {
            const userInfo = response.data.userInfo;
            const user = userInfo.user;
            
            const isLive = userInfo.stats?.roomId || user.roomId || false;
            
            const result = {
                isLive: !!isLive,
                userInfo: {
                    followerCount: userInfo.stats?.followerCount || 0,
                    followingCount: userInfo.stats?.followingCount || 0,
                    heartCount: userInfo.stats?.heartCount || 0,
                    videoCount: userInfo.stats?.videoCount || 0,
                    verified: user.verified || false,
                    roomId: userInfo.stats?.roomId || user.roomId || null
                },
                source: 'direct-api'
            };
            
            console.log(`📊 [${username}] 直接API 結果:`, result);
            return result;
        }
        
        throw new Error('APIレスポンスの形式が異常');
        
    } catch (error) {
        console.log(`❌ [${username}] 直接API エラー:`, error.message);
        throw error;
    }
}

// 統合ライブ検出関数
async function checkLiveWithAlternatives(username) {
    console.log(`🔄 [${username}] 代替ライブラリによる統合チェック開始`);
    
    const results = {
        username: username,
        timestamp: new Date().toISOString(),
        attempts: [],
        finalResult: null
    };
    
    // 方法1: 直接API（最も成功しやすい）
    try {
        const directResult = await checkLiveWithDirectAPI(username);
        results.attempts.push({
            method: 'direct-api',
            result: 'success',
            data: directResult
        });
        
        if (directResult.isLive) {
            results.finalResult = { isLive: true, source: 'direct-api', confidence: 'high' };
            return results;
        }
    } catch (directError) {
        results.attempts.push({
            method: 'direct-api',
            result: 'error',
            error: { message: directError.message }
        });
    }
    
    // 方法2: Web Scraping
    try {
        await new Promise(resolve => setTimeout(resolve, 3000)); // 3秒待機
        
        const scrapingResult = await checkLiveWithScraping(username);
        results.attempts.push({
            method: 'web-scraping',
            result: 'success',
            data: scrapingResult
        });
        
        if (scrapingResult.isLive) {
            results.finalResult = { isLive: true, source: 'web-scraping', confidence: 'medium' };
            return results;
        }
    } catch (scrapingError) {
        results.attempts.push({
            method: 'web-scraping',
            result: 'error',
            error: { message: scrapingError.message }
        });
    }
    
    // 全て失敗またはオフライン
    results.finalResult = { isLive: false, source: 'all-methods', confidence: 'high' };
    return results;
}

// =============================================================================
// TikTok Live Connector（従来）によるライブ検出
// =============================================================================

// より正確なライブ状態チェック関数
async function checkSingleUserLiveStatusAccurate(username) {
  try {
    console.log(`${username}: 正確なライブ状態チェック開始`);
    
    const testConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: false,
      processInitialData: false,
      enableWebsocketUpgrade: true,
      requestPollingIntervalMs: 1000,
      sessionId: undefined,
      clientParams: {},
      requestHeaders: {},
      websocketHeaders: {},
      requestOptions: {},
      websocketOptions: {}
    });
    
    // タイムアウト付きで接続テスト
    const connectionPromise = testConnection.connect();
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Connection timeout')), 10000);
    });
    
    await Promise.race([connectionPromise, timeoutPromise]);
    
    // 短時間待機してライブストリームの状態を確認
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    console.log(`${username}: ライブ配信中を確認（高精度チェック）`);
    testConnection.disconnect();
    
    return true;
    
  } catch (error) {
    console.log(`${username}: ライブ状態チェックエラー - ${error.message}`);
    
    // オフライン判定に該当するエラーパターンを追加
    const offlineErrors = [
      'LIVE has ended',
      'UserOfflineError',
      'User is not live',
      'Room not found',
      'Connection timeout',
      'Failed to retrieve the initial room data',
      'Failed to connect to websocket',
      'Unable to retrieve room data',
      'Room is not available',
      'Stream is not available',
      'User does not exist',
      'Private account or user not found'
    ];
    
    const isOffline = offlineErrors.some(pattern => 
      error.message.includes(pattern)
    );
    
    if (isOffline) {
      console.log(`${username}: オフライン状態を確認 (エラー: ${error.message})`);
      return false;
    }
    
    // 不明なエラーの場合はnullを返す
    console.log(`${username}: 判定不能なエラー - ${error.message}`);
    return null;
  }
}

// 単一ユーザーのライブ状態チェック関数（既存）
async function checkSingleUserLiveStatus(username) {
  try {
    console.log(`${username}: 即座ライブ状態チェック開始`);
    
    const testConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: false,
    });
    
    await testConnection.connect();
    console.log(`${username}: ライブ配信中を確認（即座チェック）`);
    
    testConnection.disconnect();
    return true;
    
  } catch (error) {
    if (error.message.includes('LIVE has ended') || error.message.includes('UserOfflineError')) {
      console.log(`${username}: ライブ終了を検出（即座チェック）`);
      
      const userData = liveData.get(username);
      if (userData) {
        userData.isLive = false;
        userData.lastUpdate = new Date().toISOString();
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        io.emit('user-disconnected', { username });
        io.emit('live-data-update', { username, data: userData });
      }
      
      return false;
    } else {
      console.log(`${username}: 即座チェック中にエラー`, error.message);
      return null;
    }
  }
}

// TikTokライブ接続関数
async function connectToTikTokLive(username) {
  try {
    const tiktokLiveConnection = new WebcastPushConnection(username, {
      enableExtendedGiftInfo: true,
    });

    // 接続イベント
    tiktokLiveConnection.connect().then(async state => {
      console.log(`${username}: 接続成功`);
      
      const initialData = createInitialUserData(username);
      liveData.set(username, initialData);
      
      // データベースに保存
      await saveUserToDatabase(username, initialData);
      
      io.emit('user-connected', { username, status: 'connected' });
      io.emit('live-data-update', { username, data: initialData });
      
      setTimeout(async () => {
        console.log(`${username}: 接続後の即座ライブ状態チェック開始`);
        await checkSingleUserLiveStatus(username);
      }, 5000);
      
    }).catch(err => {
      console.error(`${username}: 接続エラー`, err);
      liveData.delete(username);
      io.emit('user-error', { username, error: err.message });
      throw err;
    });

    // コメントイベント
    tiktokLiveConnection.on('comment', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.totalComments++;
        userData.recentComments.unshift({
          user: data.nickname,
          comment: data.comment,
          timestamp: new Date().toISOString()
        });
        
        userData.recentComments = userData.recentComments.slice(0, 10);
        userData.lastUpdate = new Date().toISOString();
        
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        io.emit('new-comment', { username, data: {
          user: data.nickname,
          comment: data.comment,
          timestamp: userData.lastUpdate
        }});
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: コメント処理エラー`, error);
      }
    });

    // ギフトイベント
    tiktokLiveConnection.on('gift', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.totalGifts++;
        
        const diamondValue = data.giftDetails?.diamond_count || data.repeatCount || 1;
        userData.totalDiamonds += diamondValue;
        
        userData.recentGifts.unshift({
          user: data.nickname,
          giftName: data.giftDetails?.name || 'Unknown Gift',
          giftId: data.giftId,
          count: data.repeatCount,
          diamonds: diamondValue,
          timestamp: new Date().toISOString()
        });
        
        userData.recentGifts = userData.recentGifts.slice(0, 10);
        userData.lastUpdate = new Date().toISOString();
        
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        io.emit('new-gift', { username, data: userData.recentGifts[0] });
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: ギフト処理エラー`, error);
      }
    });

    // 視聴者数更新
    tiktokLiveConnection.on('roomUser', async data => {
      try {
        let userData = liveData.get(username);
        if (!userData) userData = createInitialUserData(username);
        
        userData.viewerCount = data.viewerCount || 0;
        userData.lastUpdate = new Date().toISOString();
        
        liveData.set(username, userData);
        
        // データベースに保存
        await saveUserToDatabase(username, userData);
        
        io.emit('live-data-update', { username, data: userData });
      } catch (error) {
        console.error(`${username}: 視聴者数更新エラー`, error);
      }
    });

    // フォローイベント
    tiktokLiveConnection.on('follow', data => {
      try {
        io.emit('new-follow', { username, data: {
          user: data.nickname,
          timestamp: new Date().toISOString()
        }});
      } catch (error) {
        console.error(`${username}: フォロー処理エラー`, error);
      }
    });

    // 切断イベント
    tiktokLiveConnection.on('disconnected', async () => {
      console.log(`${username}: 切断`);
      try {
        const userData = liveData.get(username);
        if (userData) {
          userData.isLive = false;
          userData.lastUpdate = new Date().toISOString();
          liveData.set(username, userData);
          
          // データベースに保存
          await saveUserToDatabase(username, userData);
        }
        io.emit('user-disconnected', { username });
      } catch (error) {
        console.error(`${username}: 切断処理エラー`, error);
      }
    });

    // エラーイベント
    tiktokLiveConnection.on('error', err => {
      console.error(`${username}: エラー`, err);
      io.emit('user-error', { username, error: err.message });
    });

    connections.set(username, tiktokLiveConnection);
    return tiktokLiveConnection;

  } catch (error) {
    console.error(`${username}: 接続作成エラー`, error);
    throw error;
  }
}

// =============================================================================
// API エンドポイント
// =============================================================================

// 代替ライブラリテストAPI
app.post('/api/test-alternative-libs', async (req, res) => {
    const { username } = req.body;
    
    if (!username) {
        return res.status(400).json({ error: 'ユーザー名が必要です' });
    }
    
    const cleanUsername = username.replace('@', '').trim();
    console.log(`🔄 代替ライブラリテスト開始: ${cleanUsername}`);
    
    try {
        const results = await checkLiveWithAlternatives(cleanUsername);
        
        console.log(`📊 代替ライブラリテスト結果 [${cleanUsername}]:`, JSON.stringify(results, null, 2));
        
        res.json({
            success: true,
            username: cleanUsername,
            results: results,
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        console.error(`❌ 代替ライブラリテストエラー [${cleanUsername}]:`, error);
        res.status(500).json({ 
            error: `代替ライブラリテストエラー: ${error.message}`,
            username: cleanUsername
        });
    }
});

// 代替ライブラリ情報API
app.get('/api/alternative-lib-info', (req, res) => {
    res.json({
        success: true,
        libraries: [
            {
                name: 'axios',
                version: 'latest',
                status: 'available'
            },
            {
                name: 'cheerio',
                version: 'latest',
                status: 'available'
            }
        ],
        methods: [
            'direct-api',
            'web-scraping'
        ],
        timestamp: new Date().toISOString()
    });
});

// ライブラリ情報確認用エンドポイント
app.get('/api/library-info', (req, res) => {
    try {
        const packageInfo = require('tiktok-live-connector/package.json');
        
        res.json({
            success: true,
            library: {
                name: packageInfo.name,
                version: packageInfo.version,
                description: packageInfo.description,
                lastModified: packageInfo._time || 'unknown'
            },
            system: {
                nodeVersion: process.version,
                platform: process.platform,
                architecture: process.arch,
                environment: process.env.NODE_ENV || 'development'
            },
            timestamp: new Date().toISOString()
        });
        
    } catch (error) {
        res.status(500).json({
            error: 'ライブラリ情報の取得に失敗しました',
            details: error.message
        });
    }
});

// ユーザー追加（完全修正版）
app.post('/api/add-user', async (req, res) => {
  const { username } = req.body;
  
  console.log('ユーザー追加リクエスト受信:', req.body);
  
  if (!username) {
    console.log('エラー: ユーザー名が空');
    return res.status(400).json({ error: 'ユーザー名が必要です' });
  }
  
  const cleanUsername = username.replace('@', '').trim();
  
  if (!cleanUsername) {
    console.log('エラー: 有効なユーザー名なし');
    return res.status(400).json({ error: '有効なユーザー名を入力してください' });
  }
  
  console.log(`${cleanUsername}: ユーザー追加処理開始`);
  
  try {
    // データベースで重複チェック
    console.log(`${cleanUsername}: 重複チェック開始`);
    const existingUser = await pool.query('SELECT username FROM users WHERE username = $1', [cleanUsername]);
    
    if (existingUser.rows.length > 0) {
      console.log(`${cleanUsername}: 既に存在`);
      return res.status(400).json({ error: 'このユーザーは既に監視中です' });
    }
    
    console.log(`${cleanUsername}: 重複なし、追加処理継続`);
    
    // まずliveDataに基本データを作成
    const userData = {
      username: cleanUsername,
      isLive: false,  // 初期状態はオフライン
      viewerCount: 0,
      totalComments: 0,
      totalGifts: 0,
      totalDiamonds: 0,
      lastUpdate: new Date().toISOString(),
      recentComments: [],
      recentGifts: []
    };
    
    liveData.set(cleanUsername, userData);
    console.log(`${cleanUsername}: liveDataに追加完了`);
    
    // データベースに追加
    const insertQuery = `
      INSERT INTO users (username, status, is_live, total_diamonds, total_gifts, total_comments, viewer_count, last_live_check)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    `;
    
    const currentTime = new Date();
    
    await pool.query(insertQuery, [
      cleanUsername,
      'monitoring',
      false,
      0,
      0,
      0,
      0,
      currentTime
    ]);
    
    console.log(`${cleanUsername}: データベースに追加完了`);
    
    // 接続数制限チェック
    if (connections.size >= MAX_CONCURRENT_CONNECTIONS) {
      console.log(`${cleanUsername}: 接続制限により待機キューに追加`);
      
      // ステータスを待機中に変更
      await pool.query('UPDATE users SET status = $1 WHERE username = $2', ['waiting', cleanUsername]);
      
      connectionQueue.push(cleanUsername);
      
      // クライアントに通知
      io.emit('live-data-update', { username: cleanUsername, data: userData });
      
      return res.json({ 
        message: `${cleanUsername} を追加しました（接続待機中: ${connectionQueue.length}番目）`,
        status: 'waiting',
        queuePosition: connectionQueue.length
      });
    }
    
    // まず代替ライブラリでライブ状態をチェック
    console.log(`${cleanUsername}: 代替ライブラリでライブ状態チェック開始`);
    
    try {
      const alternativeResult = await checkLiveWithAlternatives(cleanUsername);
      
      if (alternativeResult.finalResult && alternativeResult.finalResult.isLive) {
        console.log(`${cleanUsername}: 代替ライブラリでライブ検出成功`);
        userData.isLive = true;
        liveData.set(cleanUsername, userData);
        await saveUserToDatabase(cleanUsername, userData);
        
        // TikTok Live Connector での接続も試行
        try {
          await connectToTikTokLive(cleanUsername);
          console.log(`${cleanUsername}: TikTok Live Connector接続も成功`);
        } catch (connectorError) {
          console.log(`${cleanUsername}: TikTok Live Connector失敗だが、代替ライブラリで検出済み`);
        }
        
        io.emit('user-connected', { username: cleanUsername, status: 'connected' });
        io.emit('live-data-update', { username: cleanUsername, data: userData });
        
        return res.json({ 
          message: `${cleanUsername} の監視を開始しました（ライブ配信中）`,
          status: 'monitoring',
          source: alternativeResult.finalResult.source
        });
        
      } else {
        console.log(`${cleanUsername}: 代替ライブラリでオフライン検出`);
        
        // TikTok Live Connector でも試行
        try {
          await connectToTikTokLive(cleanUsername);
          console.log(`${cleanUsername}: TikTok Live Connector接続成功`);
          
          io.emit('user-connected', { username: cleanUsername, status: 'connected' });
          io.emit('live-data-update', { username: cleanUsername, data: userData });
          
          return res.json({ 
            message: `${cleanUsername} の監視を開始しました`,
            status: 'monitoring'
          });
          
        } catch (connectError) {
          console.log(`${cleanUsername}: 全ての接続方法が失敗`);
          
          // 接続失敗でもユーザーは追加済み
          io.emit('live-data-update', { username: cleanUsername, data: userData });
          
          return res.json({
            message: `${cleanUsername} を追加しました（現在ライブ配信していません）`,
            status: 'monitoring',
            warning: '現在ライブ配信していません'
          });
        }
      }
      
    } catch (alternativeError) {
      console.log(`${cleanUsername}: 代替ライブラリエラー、従来方法を試行`);
      
      // 代替ライブラリが失敗した場合は従来の方法
      try {
        await connectToTikTokLive(cleanUsername);
        console.log(`${cleanUsername}: 従来のTikTok接続成功`);
        
        io.emit('user-connected', { username: cleanUsername, status: 'connected' });
        io.emit('live-data-update', { username: cleanUsername, data: userData });
        
        return res.json({ 
          message: `${cleanUsername} の監視を開始しました`,
          status: 'monitoring'
        });
        
      } catch (connectError) {
        console.log(`${cleanUsername}: 全ての接続方法が失敗`);
        
        io.emit('live-data-update', { username: cleanUsername, data: userData });
        
        return res.json({
          message: `${cleanUsername} を追加しました（接続に問題があります）`,
          status: 'monitoring',
          warning: '接続に問題があります'
        });
      }
    }
    
  } catch (error) {
    console.error(`${cleanUsername}: ユーザー追加エラー`, error);
    
    // エラー時はliveDataからも削除
    liveData.delete(cleanUsername);
    
    res.status(500).json({ error: `追加エラー: ${error.message}` });
  }
});

// ユーザー削除（接続解放対応）
app.post('/api/remove-user', async (req, res) => {
  const { username } = req.body;
  const cleanUsername = username.replace('@', '');
  
  try {
    // データベースから削除
    const result = await pool.query('DELETE FROM users WHERE username = $1 RETURNING *', [cleanUsername]);
    
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'ユーザーが見つかりません' });
    }
    
    // 接続を切断
    const connection = connections.get(cleanUsername);
    if (connection) {
      connection.disconnect();
      connections.delete(cleanUsername);
      console.log(`${cleanUsername}: 接続解放 (残り${connections.size})`);
    }
    
    liveData.delete(cleanUsername);
    
    // キューから削除（もしある場合）
    const queueIndex = connectionQueue.indexOf(cleanUsername);
    if (queueIndex !== -1) {
      connectionQueue.splice(queueIndex, 1);
    }
    
    io.emit('user-removed', { username: cleanUsername });
    
    // 接続枠が空いたのでキューを処理
    setTimeout(() => {
      processConnectionQueue();
    }, 1000);
    
    res.json({ 
      message: `${cleanUsername} の監視を停止しました`,
      activeConnections: connections.size,
      queueLength: connectionQueue.length
    });
  } catch (error) {
    console.error(`${cleanUsername}: 削除エラー`, error);
    res.status(500).json({ error: '削除に失敗しました' });
  }
});

// CSV一括追加（データベース対応版）
app.post('/api/upload-csv', upload.single('csvfile'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'CSVファイルが必要です' });
  }
  
  const errors = [];
  const successUsers = [];
  const waitingUsers = [];
  
  try {
    const csvData = await new Promise((resolve, reject) => {
      const data = [];
      fs.createReadStream(req.file.path)
        .pipe(csv())
        .on('data', (row) => {
          const username = Object.values(row)[0];
          if (username && username.trim()) {
            data.push(username.replace('@', '').trim());
          }
        })
        .on('end', () => resolve(data))
        .on('error', reject);
    });
    
    fs.unlinkSync(req.file.path);
    
    console.log(`CSV一括登録: ${csvData.length}件のユーザーを処理開始`);
    
    for (const username of csvData) {
      try {
        const existingUser = await pool.query(
          'SELECT username FROM users WHERE username = $1', 
          [username]
        );
        
        if (existingUser.rows.length > 0) {
          errors.push(`${username}: 既に監視中です`);
          continue;
        }
        
        if (connections.size >= MAX_CONCURRENT_CONNECTIONS) {
          await pool.query(`
            INSERT INTO users (username, status, is_live)
            VALUES ($1, 'waiting', false)
          `, [username]);
          
          connectionQueue.push(username);
          waitingUsers.push(username);
          
          console.log(`${username}: CSV経由で追加（待機キュー）`);
        } else {
          await connectToTikTokLive(username);
          successUsers.push(username);
          
          console.log(`${username}: CSV経由で追加成功`);
        }
        
      } catch (error) {
        console.error(`${username}: CSV追加エラー - ${error.message}`);
        errors.push(`${username}: ${error.message}`);
      }
    }
    
    let responseMessage = `${successUsers.length}件のユーザーを追加しました`;
    if (waitingUsers.length > 0) {
      responseMessage += `（${waitingUsers.length}件は接続待機中）`;
    }
    
    res.json({ 
      message: responseMessage,
      success: successUsers.length,
      waiting: waitingUsers.length,
      total: csvData.length,
      errors: errors.length > 0 ? errors : undefined
    });
    
  } catch (error) {
    console.error('CSV処理エラー:', error);
    
    if (fs.existsSync(req.file.path)) {
      fs.unlinkSync(req.file.path);
    }
    
    res.status(500).json({ 
      error: 'CSVファイルの処理中にエラーが発生しました: ' + error.message 
    });
  }
});

// 特定ユーザーの詳細チェック（テスト用）
app.post('/api/check-user-detailed', async (req, res) => {
  const { username } = req.body;
  
  if (!username) {
    return res.status(400).json({ error: 'ユーザー名が必要です' });
  }
  
  const cleanUsername = username.replace('@', '').trim();
  
  try {
    console.log(`${cleanUsername}: 詳細チェック開始`);
    
    // 第1段階：基本チェック
    const basicResult = await checkSingleUserLiveStatus(cleanUsername);
    
    // 第2段階：高精度チェック
    const accurateResult = await checkSingleUserLiveStatusAccurate(cleanUsername);
    
    // 現在のliveDataの状態
    const currentData = liveData.get(cleanUsername);
    
    res.json({
      username: cleanUsername,
      basicCheck: basicResult,
      accurateCheck: accurateResult,
      currentLiveData: currentData || null,
      finalStatus: accurateResult !== null ? accurateResult : basicResult,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error(`${cleanUsername}: 詳細チェックエラー`, error);
    res.status(500).json({ error: `チェックエラー: ${error.message}` });
  }
});

// 監視ユーザー一覧取得
app.get('/api/users', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT username, added_at, status, total_diamonds, total_gifts, 
             total_comments, is_live, viewer_count, last_live_check
      FROM users 
      WHERE status IN ('monitoring', 'waiting')
      ORDER BY added_at ASC
    `);
    
    const users = result.rows.map(row => ({
      username: row.username,
      addedAt: row.added_at,
      status: row.status
    }));
    
    res.json({
      users: users,
      liveData: Object.fromEntries(liveData)
    });
  } catch (error) {
    console.error('ユーザー一覧取得エラー:', error);
    res.status(500).json({ error: 'ユーザー一覧の取得に失敗しました' });
  }
});

// ランキング取得
app.get('/api/ranking', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT username, total_diamonds, total_gifts, total_comments, 
             is_live, viewer_count, last_live_check
      FROM users 
      WHERE status = 'monitoring' AND total_diamonds > 0
      ORDER BY total_diamonds DESC
    `);
    
    const dailyRanking = result.rows.map((user, index) => ({
      rank: index + 1,
      username: user.username,
      totalDiamonds: user.total_diamonds,
      totalGifts: user.total_gifts,
      totalComments: user.total_comments,
      viewerCount: user.viewer_count || 0,
      isLive: user.is_live,
      estimatedEarnings: Math.round(user.total_diamonds * 0.005 * 100) / 100,
      lastUpdate: user.last_live_check
    }));
    
    res.json({
      ranking: dailyRanking,
      totalUsers: await pool.query('SELECT COUNT(*) FROM users WHERE status IN (\'monitoring\', \'waiting\')').then(r => parseInt(r.rows[0].count)),
      activeUsers: dailyRanking.length,
      lastUpdate: new Date().toISOString()
    });
  } catch (error) {
    console.error('ランキング取得エラー:', error);
    res.status(500).json({ error: 'ランキング取得に失敗しました' });
  }
});

// 接続状況確認API
app.get('/api/connection-status', async (req, res) => {
  try {
    const totalUsers = await pool.query('SELECT COUNT(*) FROM users');
    const activeUsers = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'monitoring\'');
    const waitingUsers = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'waiting\'');
    
    res.json({
      totalUsers: parseInt(totalUsers.rows[0].count),
      activeUsers: parseInt(activeUsers.rows[0].count),
      waitingUsers: parseInt(waitingUsers.rows[0].count),
      activeConnections: connections.size,
      queueLength: connectionQueue.length,
      maxConnections: MAX_CONCURRENT_CONNECTIONS,
      availableSlots: MAX_CONCURRENT_CONNECTIONS - connections.size
    });
  } catch (error) {
    res.status(500).json({ error: '接続状況の取得に失敗しました' });
  }
});

// ライブデータ取得
app.get('/api/live-data', (req, res) => {
  res.json(Object.fromEntries(liveData));
});

// ルート設定
app.get('/', (req, res) => {
  res.json({ 
    status: 'TikTok Live Monitor API with Alternative Libraries',
    timestamp: new Date().toISOString(),
    version: '3.0.0'
  });
});

// ヘルスチェック
app.get('/health', async (req, res) => {
  try {
    await pool.query('SELECT 1');
    
    const userCount = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'monitoring\'');
    const waitingCount = await pool.query('SELECT COUNT(*) FROM users WHERE status = \'waiting\'');
    
    res.json({ 
      status: 'OK', 
      timestamp: new Date().toISOString(),
      database: 'connected',
      monitoredUsers: parseInt(userCount.rows[0].count),
      waitingUsers: parseInt(waitingCount.rows[0].count),
      activeConnections: connections.size,
      queueLength: connectionQueue.length,
      maxConnections: MAX_CONCURRENT_CONNECTIONS,
      availableSlots: MAX_CONCURRENT_CONNECTIONS - connections.size,
      features: ['postgresql', 'csv-upload', 'alternative-libraries', 'connection-management']
    });
  } catch (error) {
    res.status(500).json({
      status: 'ERROR',
      timestamp: new Date().toISOString(),
      database: 'disconnected',
      error: error.message
    });
  }
});

// 定期的なライブ状態チェック（高精度版、3分ごと）
setInterval(() => {
  checkLiveStatusAccurate();
}, 3 * 60 * 1000);

// 定期的な履歴保存（10分ごと）
setInterval(() => {
  saveBulkLiveHistory();
}, 10 * 60 * 1000);

// 段階的ライブ状態チェック（精度向上版）
async function checkLiveStatusAccurate() {
  console.log('=== 高精度ライブ状態チェック開始 ===');
  console.log(`liveData件数: ${liveData.size}`);
  
  if (liveData.size === 0) {
    console.log('⚠️ liveDataが空です');
    return;
  }
  
  for (const [username, userData] of liveData) {
    console.log(`${username}: 段階的チェック開始 (現在: ${userData.isLive ? 'ライブ中' : 'オフライン'})`);
    
    let isLive = null;
    
    // まず代替ライブラリでチェック
    try {
      const alternativeResult = await checkLiveWithAlternatives(username);
      if (alternativeResult.finalResult) {
        isLive = alternativeResult.finalResult.isLive;
        console.log(`${username}: 代替ライブラリ結果 - ${isLive ? 'ライブ中' : 'オフライン'}`);
      }
    } catch (alternativeError) {
      console.log(`${username}: 代替ライブラリエラー、従来方法を試行`);
    }
    
    // 代替ライブラリが失敗した場合は従来の方法
    if (isLive === null) {
      isLive = await checkSingleUserLiveStatus(username);
      
      if (isLive === null) {
        isLive = await checkSingleUserLiveStatusAccurate(username);
        
        if (isLive === null) {
          console.log(`${username}: 全てのチェック失敗、現在の状態を維持`);
          continue;
        }
      }
    }
    
    const previousStatus = userData.isLive;
    
    // 状態変更の処理
    if (isLive !== previousStatus) {
      console.log(`${username}: 状態変更検出 ${previousStatus ? 'ライブ中' : 'オフライン'} → ${isLive ? 'ライブ中' : 'オフライン'}`);
      
      userData.isLive = isLive;
      userData.lastUpdate = new Date().toISOString();
      liveData.set(username, userData);
      
      // データベースに保存
      await saveUserToDatabase(username, userData);
      
      // 通知送信
      if (isLive) {
        io.emit('user-connected', { username, status: 'connected' });
        io.emit('live-status-change', { 
          username, 
          status: 'online',
          timestamp: userData.lastUpdate,
          message: `${username} がライブを開始しました`
        });
        
        // TikTok接続を再開
        if (!connections.has(username) && connections.size < MAX_CONCURRENT_CONNECTIONS) {
          try {
            await connectToTikTokLive(username);
            console.log(`${username}: TikTok接続再開成功`);
          } catch (error) {
            console.error(`${username}: TikTok接続再開失敗`, error);
          }
        }
      } else {
        const existingConnection = connections.get(username);
        if (existingConnection) {
          existingConnection.disconnect();
          connections.delete(username);
        }
        
        io.emit('user-disconnected', { username });
        io.emit('live-status-change', { 
          username, 
          status: 'offline',
          timestamp: userData.lastUpdate,
          message: `${username} がライブを終了しました`
        });
        
        setTimeout(() => {
          processConnectionQueue();
        }, 1000);
      }
      
      io.emit('live-data-update', { username, data: userData });
    } else {
      console.log(`${username}: 状態変更なし (${isLive ? 'ライブ中' : 'オフライン'})`);
    }
  }
  
  console.log('=== 高精度ライブ状態チェック完了 ===');
}

// 一括履歴保存
async function saveBulkLiveHistory() {
  console.log('=== 履歴データ保存開始 ===');
  
  for (const [username, userData] of liveData) {
    try {
      await saveLiveHistory(username, userData);
    } catch (error) {
      console.error(`${username}: 履歴保存エラー`, error);
    }
  }
  
  console.log('=== 履歴データ保存完了 ===');
}

// サーバー起動
const PORT = process.env.PORT || 10000;

// データベース初期化後にサーバー起動
initializeDatabase().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    console.log(`=== TikTok Live Monitor Server (Alternative Libraries) ===`);
    console.log(`Server running on port ${PORT}`);
    console.log(`Max connections: ${MAX_CONCURRENT_CONNECTIONS}`);
    console.log(`Timestamp: ${new Date().toISOString()}`);
    console.log(`Features: PostgreSQL, CSV Upload, Alternative Libraries, Connection Management`);
    console.log(`Libraries: axios, cheerio, tiktok-live-connector`);
    console.log(`Health check: /health`);
    console.log(`API Base: /api`);
  });
}).catch(error => {
  console.error('サーバー起動エラー:', error);
  process.exit(1);
});

// エラーハンドリング
server.on('error', (err) => {
  console.error('Server error:', err);
});

process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
});

process.on('unhandledRejection', (err) => {
  console.error('Unhandled rejection:', err);
});

// 終了時の処理
process.on('SIGTERM', async () => {
  console.log('サーバーを停止しています...');
  
  connections.forEach((connection, username) => {
    console.log(`${username} の接続を切断中...`);
    connection.disconnect();
  });
  
  await pool.end();
  
  server.close(() => {
    console.log('サーバーが停止しました');
    process.exit(0);
  });
});
