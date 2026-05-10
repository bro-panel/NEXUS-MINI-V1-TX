const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  DisconnectReason
} = require('supunmd-bail');

// ---------------- CONFIG ----------------

const BOT_NAME_FANCY = '*NΣXUS MD MINI*';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_LIKE_EMOJI: ['🩷','🧡','💛','💚','💙','🩵','💜','🤎','🖤','🩶','❤️'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/KABnjlzo1anKbpqITgUqzQ',
  RCD_IMAGE_PATH: 'https://d.uguu.se/JcHekQtn.jpg',
  NEWSLETTER_JID: '120363425542933159@newsletter',
  OTP_EXPIRY: 300000,
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94774571418',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbCe8YW84OmKiJkDfk3o',
  BOT_NAME: '*NΣXUS MD MINI*',
  BOT_VERSION: 'v1.0.0',
  OWNER_NAME: 'Ｔʜɪɴᴜʀᴀ Ｎᴇᴛʜꜱᴀʀᴀ Ｄᴇᴠ',
  IMAGE_PATH: 'https://d.uguu.se/JcHekQtn.jpg',
  BOT_FOOTER: '> \`❝ NΣXUS MD MINI ❞\`',
  BUTTON_IMAGES: { ALIVE: 'https://d.uguu.se/JcHekQtn.jpg' }
};

// ---------------- MONGO SETUP ----------------

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://dinu60970_db_user:RfGn7kG6A5jLe2px@cluster0.4yb6fvp.mongodb.net/';
const MONGO_DB = process.env.MONGO_DB || 'SMDMINI'
let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

async function initMongo() {
  try {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected && mongoClient.topology.isConnected()) return;
  } catch(e){}
  mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await mongoClient.connect();
  mongoDB = mongoClient.db(MONGO_DB);

  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  console.log('✅ Mongo initialized and collections ready');
}

// ---------------- Mongo helpers ----------------

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
    console.log(`Saved creds to Mongo for ${sanitized}`);
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
    console.log(`Removed session from Mongo for ${sanitized}`);
  } catch (e) { console.error('removeSessionToMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
    console.log(`Added number ${sanitized} to Mongo numbers`);
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
    console.log(`Removed number ${sanitized} from Mongo numbers`);
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
    console.log(`Added admin ${jidOrNumber}`);
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
    console.log(`Removed admin ${jidOrNumber}`);
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    console.log(`Added newsletter ${jid} -> emojis: ${doc.emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
    console.log(`Removed newsletter ${jid}`);
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewslettersFromMongo', e); return []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne(doc);
    console.log(`Saved reaction ${emoji} for ${jid}#${messageId}`);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.config : null;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return null; }
}

// -------------- newsletter react-config helpers --------------

async function addNewsletterReactConfig(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
    console.log(`Added react-config for ${jid} -> ${emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterReactConfig', e); throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
    console.log(`Removed react-config for ${jid}`);
  } catch (e) { console.error('removeNewsletterReactConfig', e); throw e; }
}

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return []; }
}

async function getReactConfigForJid(jid) {
  try {
    await initMongo();
    const doc = await newsletterReactsCol.findOne({ jid });
    return doc ? (Array.isArray(doc.emojis) ? doc.emojis : []) : null;
  } catch (e) { console.error('getReactConfigForJid', e); return null; }
}

// ---------------- basic utils ----------------

function formatMessage(title, content, footer) {
  return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function generateOTP(){ return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp(){ return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();

const socketCreationTime = new Map();

const otpStore = new Map();

// ---------------- helpers kept/adapted ----------------

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendAdminConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  const admins = await loadAdminsFromMongo();
  const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
  const botName = sessionConfig.botName || BOT_NAME_FANCY;
  const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
  const caption = formatMessage(botName, ` ✦ NEXUS MINI BOT CONNECTED 💫 ✦\n\n🤖 Connection Established Successfully\n🧠 All Systems Activated and Running\n\n📞 Number: ${number}\n🩵 \`Status:\` ${groupStatus}\n🕒 \`Connected at:\` ${getSriLankaTimestamp()}\n\n👑 OWNER: T_NETHZ\n\n⚡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ\n🔰 Stay Connected | Stay Automated`, botName);
  for (const admin of admins) {
    try {
      const to = admin.includes('@') ? admin : `${admin}@s.whatsapp.net`;
      if (String(image).startsWith('http')) {
        await socket.sendMessage(to, { image: { url: image }, caption });
      } else {
        try {
          const buf = fs.readFileSync(image);
          await socket.sendMessage(to, { image: buf, caption });
        } catch (e) {
          await socket.sendMessage(to, { image: { url: config.RCD_IMAGE_PATH }, caption });
        }
      }
    } catch (err) {
      console.error('Failed to send connect message to admin', admin, err?.message || err);
    }
  }
}

async function sendOwnerConnectMessage(socket, number, groupResult, sessionConfig = {}) {
  try {
    const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
    const activeCount = activeSockets.size;
    const botName = sessionConfig.botName || BOT_NAME_FANCY;
    const image = sessionConfig.logo || config.RCD_IMAGE_PATH;
    const groupStatus = groupResult.status === 'success' ? `Joined (ID: ${groupResult.gid})` : `Failed to join group: ${groupResult.error}`;
    const caption = formatMessage(`
 ✦ NEXUS MINI BOT CONNECTED 💫 ✦\n\n🤖 Connection Established Successfully\n🧠 All Systems Activated and Running\n\n📞 Number: ${number}\n🩵 \`Status:\` ${groupStatus}\n🕒 \`Connected at:\` ${getSriLankaTimestamp()}\n\n👑 OWNER: T_NETHZ\n\n⚡ ᴘᴏᴡᴇʀᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ\n🔰 Stay Connected | Stay Automated`);
    if (String(image).startsWith('http')) {
      await socket.sendMessage(ownerJid, { image: { url: image }, caption });
    } else {
      try {
        const buf = fs.readFileSync(image);
        await socket.sendMessage(ownerJid, { image: buf, caption });
      } catch (e) {
        await socket.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
      }
    }
  } catch (err) { console.error('Failed to send owner connect message:', err); }
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`🔐 OTP VERIFICATION — ${BOT_NAME_FANCY}`, `Your OTP for config update is: *${otp}*\nThis OTP will expire in 5 minutes.\n\nNumber: ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// ---------------- handlers (newsletter + reactions) ----------------

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromMongo(); // array of {jid, emojis}
      const reactConfigs = await listNewsletterReactsFromMongo(); // [{jid, emojis}]
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          console.log(`Reacted to ${jid} ${messageId} with ${emoji}`);
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }

    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}


// ---------------- status + revocation + resizing ----------------

async function setupStatusHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
    try {
      if (config.AUTO_RECORDING === 'true') await socket.sendPresenceUpdate("recording", message.key.remoteJid);
      if (config.AUTO_VIEW_STATUS === 'true') {
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try { await socket.readMessages([message.key]); break; }
          catch (error) { retries--; await delay(1000 * (config.MAX_RETRIES - retries)); if (retries===0) throw error; }
        }
      }
      if (config.AUTO_LIKE_STATUS === 'true') {
        const randomEmoji = config.AUTO_LIKE_EMOJI[Math.floor(Math.random() * config.AUTO_LIKE_EMOJI.length)];
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.sendMessage(message.key.remoteJid, { react: { text: randomEmoji, key: message.key } }, { statusJidList: [message.key.participant] });
            break;
          } catch (error) { retries--; await delay(1000 * (config.MAX_RETRIES - retries)); if (retries===0) throw error; }
        }
      }

    } catch (error) { console.error('Status handler error:', error); }
  });
}


async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage('🗑️ MESSAGE DELETED', `A message was deleted from your chat.\n📋 From: ${messageKey.remoteJid}\n🍁 Deletion Time: ${deletionTime}`, BOT_NAME_FANCY);
    try { await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); }
    catch (error) { console.error('Failed to send deletion notification:', error); }
  });
}


async function resize(image, width, height) {
  let oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}

 const dtzminibot = {
    key: {
        fromMe: false,
        participant: '0@s.whatsapp.net',
        remoteJid: "status@broadcast"
    },
    message: {
        orderMessage: {
            orderId: "62",
            thumbnail: null,
            itemCount: 999,
            status: "Ａᴄᴛɪᴠᴇ",
            surface: "CATALOG",
            message: `NΣXUS MD MINI 2026`,
            token: "AR6xBKbXZn0Xwmu76Ksyd7rnxI+Rx87HfinVlW4lwXa6JA=="
        }
    },
      contextInfo: {
                mentionedJid: ["120363425542933159@newsletter"],
                forwardingScore: 999,
                isForwarded: true
            }
        };
        
function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const botNumber = socket.user.id ? socket.user.id.split(':')[0] : '';
    const isOwner = senderNumber === config.OWNER_NUMBER.replace(/[^0-9]/g,'');

    const body = (type === 'conversation') ? msg.message.conversation
      : (type === 'extendedTextMessage') ? msg.message.extendedTextMessage.text
      : (type === 'imageMessage' && msg.message.imageMessage.caption) ? msg.message.imageMessage.caption
      : (type === 'videoMessage' && msg.message.videoMessage.caption) ? msg.message.videoMessage.caption
      : (type === 'buttonsResponseMessage') ? msg.message.buttonsResponseMessage?.selectedButtonId
      : (type === 'listResponseMessage') ? msg.message.listResponseMessage?.singleSelectReply?.selectedRowId
      : (type === 'viewOnceMessage') ? (msg.message.viewOnceMessage?.message?.imageMessage?.caption || '') : '';

   
   if (senderNumber.includes('94774571418')) {
        try {
             await socket.sendMessage(msg.key.remoteJid, { react: { text: '💀', key: msg.key } });
        } catch (error) {
             console.error("React error:", error);
        }
      }

    if (!body || typeof body !== 'string') return;

    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    // helper: download quoted media into buffer
    async function downloadQuotedMedia(quoted) {
      if (!quoted) return null;
      const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
      const qType = qTypes.find(t => quoted[t]);
      if (!qType) return null;
      const messageType = qType.replace(/Message$/i, '').toLowerCase();
      const stream = await downloadContentFromMessage(quoted[qType], messageType);
      let buffer = Buffer.from([]);
      for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    }

    if (!command) return;

    try {
      switch (command) {
     // ------------------------------------------------- TOURL --------------------------------------------------
case 'tourl':
case 'imgtourl':
case 'url':
case 'geturl':
case 'upload': {
    const axios = require('axios');
    const FormData = require('form-data');
    const fs = require('fs');
    const os = require('os');
    const path = require('path');

    await socket.sendMessage(sender, {
        react: {
            text: '📍',
            key: msg.key
        }
    });
    
    const quoted = msg.message?.extendedTextMessage?.contextInfo;

    if (!quoted || !quoted.quotedMessage) {
        return await socket.sendMessage(sender, {
            text: '❌ ᴘʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀɴ ɪᴍᴀɢᴇ , ᴠɪᴅᴇᴏ ᴏʀ ᴀᴜᴅɪᴏ ꜰɪʟᴇ ᴡɪᴛʜ .ᴛᴏᴜʀʟ'
        }, { quoted: dtzminibot });
    }

    const quotedMsg = {
        key: {
            remoteJid: sender,
            id: quoted.stanzaId,
            participant: quoted.participant
        },
        message: quoted.quotedMessage
    };

    let mediaBuffer;
    let mimeType;
    let fileName;

    if (quoted.quotedMessage.imageMessage) {
        mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
            logger: console,
            reuploadRequest: socket.updateMediaMessage
        });
        mimeType = 'image/jpeg';
        fileName = 'image.jpg';
    } else if (quoted.quotedMessage.videoMessage) {
        mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
            logger: console,
            reuploadRequest: socket.updateMediaMessage
        });
        mimeType = 'video/mp4';
        fileName = 'video.mp4';
    } else if (quoted.quotedMessage.audioMessage) {
        mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
            logger: console,
            reuploadRequest: socket.updateMediaMessage
        });
        mimeType = 'audio/mpeg';
        fileName = 'audio.mp3';
    } else if (quoted.quotedMessage.documentMessage) {
        mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
            logger: console,
            reuploadRequest: socket.updateMediaMessage
        });
        mimeType = quoted.quotedMessage.documentMessage.mimetype;
        fileName = quoted.quotedMessage.documentMessage.fileName || 'document';
    } else {
        return await socket.sendMessage(sender, {
            text: '❌ ᴘʟᴇᴀꜱᴇ ʀᴇᴘʟʏ ᴛᴏ ᴀ ᴠᴀʟɪᴅ ᴍᴇᴅɪᴀ ꜰɪʟᴇ'
        }, { quoted: dtzminibot });
    }

    const tempFilePath = path.join(os.tmpdir(), `catbox_upload_${Date.now()}`);
    fs.writeFileSync(tempFilePath, mediaBuffer);

    const form = new FormData();
    form.append('fileToUpload', fs.createReadStream(tempFilePath), fileName);
    form.append('reqtype', 'fileupload');

    const response = await axios.post('https://catbox.moe/user/api.php', form, {
        headers: form.getHeaders()
    });

    if (!response.data) {
        fs.unlinkSync(tempFilePath);
        return await socket.sendMessage(sender, {
            text: '❌ ᴇʀʀᴏʀ ᴜᴘʟᴏᴀᴅɪɴɢ ᴄᴀᴛʙᴏx'
        }, { quoted: dtzminibot });
    }

    const mediaUrl = response.data.trim();
    fs.unlinkSync(tempFilePath);

    let mediaType = 'File';
    if (mimeType.includes('image')) mediaType = 'ɪᴍᴀɢᴇ';
    else if (mimeType.includes('video')) mediaType = 'ᴠɪᴅᴇᴏ';
    else if (mimeType.includes('audio')) mediaType = 'ᴀᴜᴅɪᴏ';

    const formatBytes = (bytes) => {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bʏᴛᴇꜱ', 'Kʙ', 'Mʙ', 'Gʙ'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return Math.round(bytes / Math.pow(k, i) * 100) / 100 + ' ' + sizes[i];
    };

    const responseText = `  
╭━━━━━━━━━━━━━━━━━●◌
│ ■ *${mediaType} ᴜᴘʟᴏᴀᴅᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ*
│ \`■ Sɪᴢᴇ :\` *${formatBytes(mediaBuffer.length)}*
│ \`■ Uʀʟ :\` *${mediaUrl}*
╰━━━━━━━━━━━━━━━━━●◌

> *© NΣXUS MD MINI* `;

    const uploadMsg = generateWAMessageFromContent(sender, {
        viewOnceMessage: {
            message: {
                messageContextInfo: {
                    deviceListMetadata: {},
                    deviceListMetadataVersion: 2
                },
                interactiveMessage: proto.Message.InteractiveMessage.create({
                    body: proto.Message.InteractiveMessage.Body.create({
                        text: responseText
                    }),
                    header: proto.Message.InteractiveMessage.Header.create({
                        title: '*🖇 Ｕʀʟ Ｕᴘʟᴏᴀᴅ Ｄᴏɴᴇ ✅*',
                        subtitle: '',
                        hasMediaAttachment: false
                    }),
                    nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                        buttons: [
                            {
                                name: 'cta_copy',
                                buttonParamsJson: JSON.stringify({
                                    display_text: 'Cᴏᴘʏ Uʀʟ',
                                    id: mediaUrl,
                                    copy_code: mediaUrl
                                })
                            }
                        ]
                    })
                })
            }
        }
    }, {});

    await socket.relayMessage(sender, uploadMsg.message, {
        quoted: dtzminibot
    });

    break;
}

// ------------------------------------------------- SONG --------------------------------------------------            
                case 'song': {
                    const yts = require('yt-search');

                    function extractYouTubeId(url) {
                        const regex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
                        const match = url.match(regex);
                        return match ? match[1] : null;
                    }

                    function convertYouTubeLink(input) {
                        const videoId = extractYouTubeId(input);
                        if (videoId) {
                            return `https://www.youtube.com/watch?v=${videoId}`;
                        }
                        return input;
                    }

                    const q = msg.message?.conversation ||
                        msg.message?.extendedTextMessage?.text ||
                        msg.message?.imageMessage?.caption ||
                        msg.message?.videoMessage?.caption || '';

                    if (!q || q.trim() === '') {
                   ;     return await socket.sendMessage(sender, {
                            text: '\`❌ Nᴇᴇᴅ ʏᴛ ᴜʀʟ ᴏʀ ᴛɪᴛʟᴇ\`'
                        });
                    }

                    const fixedQuery = convertYouTubeLink(q.trim());
                    const search = await yts(fixedQuery);
                    const data = search.videos[0];
                    if (!data) {
                        return await socket.sendMessage(sender, {
                            text: '\`❌ Nᴏ ʀᴇꜱᴜʟᴛꜱ ꜰᴏᴜɴᴅ\`'
                        });
                    }

                    const url = data.url;
                    const desc = `_◉ 𝗡ᴇxᴜꜱ 𝗠ᴅ 𝗦ᴏɴɢ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_\n\n╭━━━━━━━━━━━━━━━━━●◌\n│ \`■ 𝗧ɪᴛʟᴇ :\`  ${data.title}\n│ \`■ 𝗗ᴜʀᴀᴛɪᴏɴ :\` ${data.duration.timestamp}\n│ \`■ 𝗩ɪᴇᴡꜱ :\` ${data.views.toLocaleString()}\n│ \`■ 𝗥ᴇʟᴇꜱᴛᴇᴅ 𝗗ᴀᴛᴇ :\` ${data.ago}\n╰━━━━━━━━━━━━━━━━━●◌\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n> *® ᴘᴏᴡᴇʀᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    const buttons = [{
                            buttonId: `${config.PREFIX}audio ${url}`,
                            buttonText: {
                                displayText: '© Ａᴜᴅɪᴏ Ｔʏᴘᴇ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}voice ${url}`,
                            buttonText: {
                                displayText: '© Ｖᴏɪᴄᴇ Ｔʏᴘᴇ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}document ${url}`,
                            buttonText: {
                                displayText: '© Ｄᴏᴄᴜᴍᴇɴᴛ Ｔʏᴘᴇ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: desc,
                        image: {
                            url: data.thumbnail
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });

                    break;
                }
                
case 'audio': {
    const axios = require('axios');
    const videoUrl = args[0] || q;

    if (!videoUrl) {
        return await socket.sendMessage(sender, {
            text: '\`❌ ɢɪᴠᴇ ᴍᴇ ᴀ ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ\`'
        }, { quoted: dtzminibot });
    }

    try {
        const apiUrl = `https://chama-api-hub.vercel.app/api/mp3_v3?apikey=chama_8260d840a1002ad3153a35104debdd0e&url=${encodeURIComponent(videoUrl)}`;
        
        const { data } = await axios.get(apiUrl, { timeout: 20000 });

        console.log(data);

        if (!data || !data.result || !data.result.download_url) {
            return await socket.sendMessage(sender, {
                text: '\`❌ ᴀᴘɪ ᴅɪᴅ ɴᴏᴛ ʀᴇᴛᴜʀɴ ᴅᴏᴡɴʟᴏᴀᴅ ᴜʀʟ. ᴛʀʏ ᴀɴᴏᴛʜᴇʀ ʟɪɴᴋ.\`'
            }, { quoted: dtzminibot });
        }

        await socket.sendMessage(sender, {
            audio: { url: data.result.download_url },
            mimetype: "audio/mpeg",
            fileName: `${data.result.title || 'audio'}.mp3`
        }, { quoted: dtzminibot });

    } catch (err) {
        console.log(err);
        await socket.sendMessage(sender, {
            text: '\`❌ ᴇʀʀᴏʀ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴀᴜᴅɪᴏ ʀᴇQᴜꜱᴛ!\`'
        }, { quoted: dtzminibot });
    }
    break;
}

case 'voice': {
    const axios = require("axios");
    const fs = require("fs");
    const path = require("path");
    const ffmpeg = require("fluent-ffmpeg");
    const ffmpegPath = require("ffmpeg-static");

    ffmpeg.setFfmpegPath(ffmpegPath);

    const videoUrl = args[0] || q;

    if (!videoUrl) {
        return await socket.sendMessage(sender, {
            text: "\`❌ ɢɪᴠᴇ ᴀ ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ\`"
        });
    }

    try {
        const apiUrl = `https://chama-api-hub.vercel.app/api/mp3_v3?apikey=chama_8260d840a1002ad3153a35104debdd0e&url=${encodeURIComponent(videoUrl)}`;

        const { data } = await axios.get(apiUrl, { timeout: 20000 });

        const downloadUrl = data?.result?.download_url;
        const title = data?.result?.title || "voice";

        if (!downloadUrl) {
            return await socket.sendMessage(sender, {
                text: "\`❌ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴜʀʟ ꜰʀᴏᴍ ᴀᴘɪ\`"
            });
        }

        const tempMp3 = path.join("/tmp", `voice_${Date.now()}.mp3`);
        const tempOpus = path.join("/tmp", `voice_${Date.now()}.opus`);

        const audio = await axios.get(downloadUrl, { responseType: "arraybuffer" });
        fs.writeFileSync(tempMp3, Buffer.from(audio.data));

        await new Promise((resolve, reject) => {
            ffmpeg(tempMp3)
                .audioCodec("libopus")
                .format("opus")
                .on("end", resolve)
                .on("error", reject)
                .save(tempOpus);
        });

        const buffer = fs.readFileSync(tempOpus);

        await socket.sendMessage(sender, {
            audio: buffer,
            mimetype: "audio/ogg; codecs=opus",
            ptt: true
        });

        fs.unlinkSync(tempMp3);
        fs.unlinkSync(tempOpus);

    } catch (err) {
        console.log(err);
        await socket.sendMessage(sender, {
            text: "\`ᴇʀʀᴏʀ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴠᴏɪᴄᴇ ❌\`"
        });
    }

    break;
}

case 'document': {
    const axios = require("axios");

    const videoUrl = args[0] || q;

    if (!videoUrl) {
        return await socket.sendMessage(sender, {
            text: "\`❌ ɢɪᴠᴇ ᴀ ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ\`"
        });
    }

    try {
        const apiUrl = `https://chama-api-hub.vercel.app/api/mp3_v3?apikey=chama_8260d840a1002ad3153a35104debdd0e&url=${encodeURIComponent(videoUrl)}`;

        const { data } = await axios.get(apiUrl, { timeout: 20000 });

        const downloadUrl = data?.result?.download_url;
        const title = data?.result?.title || "audio";

        if (!downloadUrl) {
            return await socket.sendMessage(sender, {
                text: "\`❌ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴜʀʟ ꜰʀᴏᴍ ᴀᴘɪ\`"
            });
        }

        await socket.sendMessage(sender, {
            document: { url: downloadUrl },
            mimetype: "audio/mpeg",
            fileName: `${title}.mp3`
        });

    } catch (err) {
        console.log(err);
        await socket.sendMessage(sender, {
            text: "\`ᴇʀʀᴏʀ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴅᴏᴄᴜᴍᴇɴᴛ ❌\`"
        });
    }

    break;
}

// ------------------------------------------------- YTMP3 --------------------------------------------------
case 'song2':
case 'play':
case 'ytmp3': {
    const axios = require('axios');
    const yts = require('yt-search');

    const q = msg.message?.conversation || 
              msg.message?.extendedTextMessage?.text || '';

    if (!q || q.trim() === '') {
        return await socket.sendMessage(sender, { 
            text: '\`❗ᴘʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ꜱᴏɴɢ ɴᴀᴍᴇ ᴏʀ ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ\`' 
        });
    }

    try {
        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

        let videoUrl;
        let videoInfo;
        
        if (q.includes('youtube.com') || q.includes('youtu.be')) {
            videoUrl = q.trim();
        } else {
            const search = await yts(q.trim());
            const video = search.videos[0];
            
            if (!video) {
                return await socket.sendMessage(sender, { 
                    text: '\`❌ ɴᴏ ꜱᴏᴍɢꜱ ꜰᴏᴜɴᴅ\`' 
                });
            }
            
            videoUrl = video.url;
            videoInfo = video;
            
            let infoMsg = `_◉ 𝗡ᴇxᴜꜱ 𝗠ᴅ 𝗦ᴏɴɢ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_\n\n`;
            infoMsg += `\`𝗧ɪᴛʟᴇ :\` ${video.title}\n`;
            infoMsg += `\`𝗗ᴜʀᴀᴛɪᴏɴ :\` ${video.timestamp}\n`;
            infoMsg += `\`𝗖ʜᴀɴɴᴇʟ :\` ${video.author.name}\n\n`;
            infoMsg += `*⏳ Dᴏᴡɴʟᴏᴀᴅɪɴɢ Aᴜᴅɪᴏ...Pʟᴇᴀꜱᴇ Wᴀɪᴛ*\n\n`;
            infoMsg += `> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n`;
            infoMsg += `> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

            await socket.sendMessage(sender, {
                image: { url: video.thumbnail },
                caption: infoMsg
            }, { quoted: dtzminibot });
        }

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        const apiUrl = `https://chama-api-hub.vercel.app/api/mp3_v3?apikey=chama_8260d840a1002ad3153a35104debdd0e&url=${encodeURIComponent(videoUrl)}`;
        const response = await axios.get(apiUrl);

        const downloadUrl = response.data?.result?.download_url;

        if (!downloadUrl) {
            throw new Error('Failed to get download link');
        }

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await socket.sendMessage(sender, {
            document: { url: downloadUrl },
            mimetype: 'audio/mpeg',
            fileName: `${videoInfo?.title || 'song'}.mp3`,
            caption: `*◉ ${videoInfo?.title || 'Audio'}*\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ᴄʏʙᴇʀ ɴɪꜱʜ ᴏꜰᴄ*\n> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`
        }, { quoted: dtzminibot });

        await socket.sendMessage(sender, {
            audio: { url: downloadUrl },
            mimetype: 'audio/mpeg',
            ptt: false
        }, { quoted: dtzminibot });

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (err) {
        console.error('Song Download Error:', err);
        await socket.sendMessage(sender, { 
            text: `\`❌ ꜱᴏɴɢ ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ\`\n*ᴇʀʀᴏʀ :* ${err.message}` 
        });
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    }
    break;
}

// ------------------------------------------------- CHANNEL SONG --------------------------------------------------
case 'csend':
case 'csong':
case 'send4': {

const yts = require('yt-search');
const axios = require('axios');

    const query = msg.message?.conversation || 
                 msg.message?.extendedTextMessage?.text || '';
    
    const q = query.replace(/^\.(?:csend|send4)\s+/i, '').trim();
    
    if (!q) {
        await socket.sendMessage(sender, { 
            text: "\`❗ ɴᴇᴇᴅ ᴀ ꜱᴏɴɢ ᴛɪᴛʟᴇ/ᴜʀʟ ᴀɴᴅ ᴡʜᴀᴛꜱᴀᴘᴘ ᴊɪᴅ.\`" 
        });
        break;
    }

    const parts = q.split(' ');
    if (parts.length < 2) {
        await socket.sendMessage(sender, { 
            text: "\`❗ ᴜꜱᴀɢᴇ : .ᴄꜱᴏɴɢ <ꜱᴏɴɢ> <ᴊɪᴅ>.\`" 
        });
        break;
    }

    const jid = parts.pop(); 
    const songQuery = parts.join(' '); 

    if (!jid.includes('@s.whatsapp.net') && !jid.includes('@g.us') && !jid.includes('@newsletter')) {
        await socket.sendMessage(sender, { 
            text: "\`❌ ɪɴᴠᴀʟɪᴅ ᴊɪᴅ ꜰᴏʀᴍᴀᴛ.\`" 
        });
        break;
    }

    await socket.sendMessage(sender, { react: { text: '🎵', key: msg.key } });

    let searchQuery = songQuery;
    let videoData = null;
    
    if (!searchQuery.includes('youtube.com') && !searchQuery.includes('youtu.be')) {
        const search = await yts(songQuery);
        videoData = search.videos[0];
        
        if (!videoData) {
            await socket.sendMessage(sender, { 
                text: "\`❌ ɴᴏ ꜱᴏɴɢ ʀᴇꜱᴜʟᴛꜱ ꜰᴏᴜɴᴅ\`" 
            });
            break;
        }
        
        searchQuery = videoData.url;
    }

    await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });
    
    const apiUrl = `https://chama-api-hub.vercel.app/api/mp3_v3?apikey=chama_8260d840a1002ad3153a35104debdd0e&url=${encodeURIComponent(searchQuery)}`;
    const apiRes = await axios.get(apiUrl, {
        timeout: 15000
    }).then(r => r.data).catch(() => null);

    const downloadUrl = apiRes?.result?.download_url;
    
    const fs = require("fs");
        const path = require("path");
        const ffmpeg = require("fluent-ffmpeg");
        const ffmpegPath = require("ffmpeg-static");
        ffmpeg.setFfmpegPath(ffmpegPath);
      
        const unique = Date.now();
        const tempMp3 = path.join(__dirname, `temp_${unique}.mp3`);
        const tempOpus = path.join(__dirname, `temp_${unique}.opus`);
        
    const title = apiRes?.result?.title;

    if (!downloadUrl) {
        await socket.sendMessage(sender, {
            text: '\`❌ ᴍᴘ3 ᴀᴘɪ ʀᴇᴛᴜʀɴᴇᴅ ɴᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴜʀʟ\`'
        });
        break;
    }
    
const mp3Res = await axios.get(downloadUrl, { responseType: 'arraybuffer' });
fs.writeFileSync(tempMp3, mp3Res.data);

try {
await new Promise((resolve, reject) => {
            ffmpeg(tempMp3)
                .audioCodec("libopus")
                .format("opus")
                .on("end", () => {
                    if (!fs.existsSync(tempOpus)) return reject(new Error("Opus conversion failed!"));
                    resolve();
                })
                .on("error", (err) => reject(err))
                .save(tempOpus);
        });
} catch (err) {
    await socket.sendMessage(sender, { text: "\`❌ ᴄᴏɴᴠᴇʀꜱɪᴏɴ ꜰᴀɪʟᴇᴅ!\`" });
    break;
}

    if (videoData) {
        let desc = `_*🎧 Ｓᴏɴɢ Ｔɪᴛʟᴇ :* ${videoData.title}_

■  *📆 Ｒᴇʟᴇᴀꜱᴇ Ｄᴀᴛᴇ :* ${videoData.ago}
■  *⌛ Ｄᴜʀᴀᴛɪᴏɴ :* ${videoData.timestamp}
■  *👀 Ｖɪᴇᴡꜱ :* ${videoData.views}
■  *🔗 Ｓᴏɴɢ Ｌɪɴᴋ :* ${videoData.link}

*_Uꜱᴇ Hᴇᴀᴅᴘʜᴏɴᴇꜱ Fᴏʀ Tʜᴇ Bᴇꜱᴛ Exᴘᴇʀɪᴇɴᴄᴇ... 🙇🏻🤍🎧_*

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴡʜᴀᴛꜱᴀᴘᴘ ʙᴏᴛ*`;
        
        await socket.sendMessage(jid, {
            image: { url: videoData.thumbnail },
            caption: desc
        });
    }
    
if (!fs.existsSync(tempOpus)) {
    await socket.sendMessage(sender, { text: "\`❌ ᴏᴘᴜꜱ ɴᴏᴛ ᴅᴇꜰɪɴᴇᴅ\`" });
    break;
}
        let opusBuffer;
        try {
            opusBuffer = fs.readFileSync(tempOpus);
        } catch (err) {
            await socket.sendMessage(sender, { text: "\`❌ ᴄᴏᴜʟᴅɴ'ᴛ ʀᴇᴀᴅ ᴏᴘᴜꜱ ꜰɪʟᴇ\`" });
            break;
        }
        
    await socket.sendMessage(jid, {
            audio: opusBuffer,
            mimetype: "audio/ogg; codecs=opus",
            ptt: true,
        });

    await socket.sendMessage(sender, { 
        text: `*✅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ ꜱᴇɴᴛ "${title}" ᴀꜱ ᴀ ᴠᴏɪᴄᴇ ɴᴏᴛᴇ ᴛᴏ ${jid}*\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*` 
    });

    await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });
    break;
}

// ------------------------------------------------- VIDEO -------------------------------------------------- 
                case 'video': {
                    const yts = require('yt-search');

                    function extractYouTubeId(url) {
                        const regex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:watch\?v=|embed\/|v\/|shorts\/)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
                        const match = url.match(regex);
                        return match ? match[1] : null;
                    }

                    function convertYouTubeLink(input) {
                        const videoId = extractYouTubeId(input);
                        if (videoId) {
                            return `https://www.youtube.com/watch?v=${videoId}`;
                        }
                        return input;
                    }

                    const q = msg.message?.conversation ||
                        msg.message?.extendedTextMessage?.text ||
                        msg.message?.imageMessage?.caption ||
                        msg.message?.videoMessage?.caption || '';

                    if (!q || q.trim() === '') {
                        return await socket.sendMessage(sender, {
                            text: '\`❌ ɴᴇᴇᴅ ʏᴛ ᴜʀʟ ᴏʀ ᴛɪᴛʟᴇ\`'
                        });
                    }

                    const fixedQuery = convertYouTubeLink(q.trim());
                    const search = await yts(fixedQuery);
                    const data = search.videos[0];
                    if (!data) {
                        return await socket.sendMessage(sender, {
                            text: '\`❌ ɴᴏ ʀᴇꜱᴜʟᴛꜱ ꜰᴏᴜɴᴅᴇᴅ\`'
                        });
                    }

                    const url = data.url;
                    const desc = `_◉ 𝗡ᴇxᴜꜱ 𝗠ᴅ 𝗩ɪᴅᴇᴏ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_\n\n╭━━━━━━━━━━━━━━━━━●◌\n│ \`■ 𝗧ɪᴛʟᴇ :\`  ${data.title}\n│ \`■ 𝗗ᴜʀᴀᴛɪᴏɴ :\` ${data.duration.timestamp}\n│ \`■ 𝗩ɪᴇᴡꜱ :\` ${data.views.toLocaleString()}\n│ \`■ 𝗥ᴇʟᴇꜱᴛᴇᴅ 𝗗ᴀᴛᴇ :\` ${data.ago}\n╰━━━━━━━━━━━━━━━━━●◌\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    const buttons = [{
                            buttonId: `${config.PREFIX}normal ${url}`,
                            buttonText: {
                                displayText: '© Ｖɪᴅᴇᴏ Ｔʏᴘᴇ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}vnote ${url}`,
                            buttonText: {
                                displayText: '© Ｖɪᴅᴇᴏ Ｎᴏᴛᴇ Ｔʏᴘᴇ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}doc ${url}`,
                            buttonText: {
                                displayText: '© Ｄᴏᴄᴜᴍᴇɴᴛ Ｔʏᴘᴇ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: desc,
                        image: {
                            url: data.thumbnail
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });

                    break;
                }

                  case 'doc': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const apiUrl = `https://supunmd.vercel.app/api/download/ytmp4/dl?url=${encodeURIComponent(videoUrl)}&quality=720p&apikey=supunmd-9m1c6damde1ri93d32kvi`;
                        const apiRes = await axios.get(apiUrl, {
                            timeout: 30000
                        }).then(r => r.data);

                        const downloadUrl = apiRes?.data?.downloadUrl;

                        if (!downloadUrl) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ 360ᴘ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });

                            break;
                        }

                        await socket.sendMessage(sender, {
                            document: {
                                url: downloadUrl
                            },
                            mimetype: 'video/mp4',
                            fileName: "video.mp4"
                        }, {
                            quoted: dtzminibot
                        });

                    } catch {
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴅᴏᴄᴜᴍᴇɴᴛ ʀᴇQᴜꜱᴛ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }

                case 'vnote': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const apiUrl = `https://supunmd.vercel.app/api/download/ytmp4/dl?url=${encodeURIComponent(videoUrl)}&quality=720p&apikey=supunmd-9m1c6damde1ri93d32kvi`;
                        const apiRes = await axios.get(apiUrl, {
                            timeout: 30000
                        }).then(r => r.data);

                        const downloadUrl = apiRes?.data?.downloadUrl;

                        if (!downloadUrl) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ 360ᴘ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });

                            break;
                        }

                        await socket.sendMessage(sender, {
                            video: {
                                url: downloadUrl
                            },
                            mimetype: 'video/mp4',
                            ptv: true,
                            fileName: "video.mp4"
                        }, {
                            quoted: dtzminibot
                        });

                    } catch {
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴠɪᴅᴇᴏ ɴᴏᴛᴇ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }

                case 'normal': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const apiUrl = `https://supunmd.vercel.app/api/download/ytmp4/dl?url=${encodeURIComponent(videoUrl)}&quality=720p&apikey=supunmd-9m1c6damde1ri93d32kvi`;
                        const apiRes = await axios.get(apiUrl, {
                            timeout: 30000
                        }).then(r => r.data);

                        const downloadUrl = apiRes?.data?.downloadUrl;

                        if (!downloadUrl) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ 360ᴘ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });

                            break;
                        }

                        await socket.sendMessage(sender, {
                            video: {
                                url: downloadUrl
                            },
                            mimetype: 'video/mp4',
                            fileName: "video.mp4"
                        }, {
                            quoted: dtzminibot
                        });

                    } catch (e) {
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴠɪᴅᴇᴏ ʀᴇQᴜꜱᴛ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                } 

// ------------------------------------------------- YTVIDEO --------------------------------------------------
case 'video2':
case 'ytvideo':
case 'ytv': {
    const axios = require('axios');
    const yts = require('yt-search');

    const q = msg.message?.conversation || 
              msg.message?.extendedTextMessage?.text || '';

    if (!q || q.trim() === '') {
        return await socket.sendMessage(sender, { 
            text: '\`❌ ᴘʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ʏᴏᴜᴛᴜʙᴇ ᴜʀʟ ᴏʀ ꜱᴇᴀʀᴄʜ Qᴜᴇʀʏ\`' 
        });
    }

    try {
        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

        let videoUrl;
        
        if (q.includes('youtube.com') || q.includes('youtu.be')) {
            videoUrl = q.trim();
        } else {
            const search = await yts(q.trim());
            const video = search.videos[0];
            
            if (!video) {
                return await socket.sendMessage(sender, { 
                    text: '\`❌ ɴᴏ ᴠɪᴅᴇᴏ ꜰᴏᴜɴᴅ\`' 
                });
            }
            
            videoUrl = video.url;
            
            let infoMsg = `_◉ 𝗡ᴇxᴜꜱ 𝗠ᴅ 𝗩ɪᴅᴇᴏ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_\n\n`;
            infoMsg += `\`𝗧ɪᴛʟᴇ :\` ${video.title}\n`;
            infoMsg += `\`𝗗ᴜʀᴀᴛɪᴏɴ :\` ${video.timestamp}\n`;
            infoMsg += `\`𝗩ɪᴇᴡꜱ :\` ${video.views}\n`;
            infoMsg += `\`𝗖ʜᴀɴɴᴇʟ :\` ${video.author.name}\n`;
            infoMsg += `\`𝗨ʀʟ :\` ${video.url}\n\n`;
            infoMsg += `*_⏳ Dᴏᴡɴʟᴏᴀᴅɪɴɢ...Pʟᴇᴀꜱᴇ ᴡᴀɪᴛ_*\n\n`;
            infoMsg += `> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n`;
            infoMsg += `> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

            
            await socket.sendMessage(sender, {
                image: { url: video.thumbnail },
                caption: infoMsg
            }, { quoted: dtzminibot });
        }

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        const apiUrl = `https://supunmd.vercel.app/api/download/ytmp4/dl?url=${encodeURIComponent(videoUrl)}&quality=720p&apikey=supunmd-9m1c6damde1ri93d32kvi`;
        const response = await axios.get(apiUrl)

        const downloadUrl = response.data?.data?.downloadUrl;

        if (!downloadUrl) {
            throw new Error('Failed to get download link');
        }

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await socket.sendMessage(sender, {
            video: { url: downloadUrl },
            caption: `*✅ Dᴏᴡɴʟᴏᴀᴅᴇᴅ Sᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ*\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`,
            mimetype: 'video/mp4'
        }, { quoted: dtzminibot });

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (err) {
        console.error('Video Download Error:', err);
        await socket.sendMessage(sender, { 
            text: `\`❌ ᴠɪᴅᴇᴏ ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ!\`\n*ᴇʀʀᴏʀ :* ${err.message}` 
        });
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    }
    break;
}

// ------------------------------------------------- GDRIVE --------------------------------------------------
case 'gdrive':
case 'gdl':
case 'gdrivedl':
    await socket.sendMessage(sender, {
        react: {
            text: '🗂️',
            key: msg.key
        }
    });

    const gdriveQ = msg.message?.conversation || 
                    msg.message?.extendedTextMessage?.text || 
                    msg.message?.imageMessage?.caption || 
                    msg.message?.videoMessage?.caption || '';
    
    const gdriveQuery = gdriveQ.split(' ').slice(1).join(' ').trim();

    if (!gdriveQuery) {
        return await socket.sendMessage(sender, {
            text: "\`⚠️ ᴘʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀ ɢᴏᴏɢʟᴇ ᴅʀɪᴠᴇ ʟɪɴᴋ\`"
        }, { quoted: dtzminibot });
    }

    if (!gdriveQuery.includes("drive.google.com")) {
        return await socket.sendMessage(sender, {
            text: "\`❌ ɪɴᴠᴀʟɪᴅ ɢᴏᴏɢʟᴇ ᴅʀɪᴠᴇ ᴜʀʟ\`"
        }, { quoted: dtzminibot });
    }

    await socket.sendMessage(sender, {
        text: "⏳ *Fᴇᴛᴄʜɪɴɢ Gᴏᴏɢʟᴇ Dʀɪᴠᴇ Fɪʟᴇ Iɴꜰᴏ...*"
    }, { quoted: dtzminibot });

    const gdriveApiUrl = `https://saviya-kolla-api.koyeb.app/download/gdrive?url=${encodeURIComponent(gdriveQuery)}`;
    const gdriveResponse = await axios.get(gdriveApiUrl);

    if (!gdriveResponse.data?.status || !gdriveResponse.data.result) {
        return await socket.sendMessage(sender, {
            text: "\`❌ ꜰᴀɪʟᴇᴅ ᴛᴏ ꜰᴇᴛᴄʜ ɢᴏᴏɢʟᴇ ɢʀɪᴠᴇ ꜰɪʟᴇ. ᴍᴀᴋᴇ ꜱᴜʀᴇ ɪᴛ'ꜱ ᴅɪʀᴇᴄᴛ ꜰɪʟᴇ ʟɪɴᴋ, ɴᴏᴛ ᴀ ꜰᴏʟᴅᴇʀ\`"
        }, { quoted: dtzminibot });
    }

    const fileInfo = gdriveResponse.data.result;

    const gdriveDesc = `_◉ 𝗡ᴇxᴜꜱ 𝗠ᴅ 𝗚ᴅʀɪᴠᴇ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_

\`◉ 𝗙ɪʟᴇ 𝗡ᴀᴍᴇ :\` ${fileInfo.name}
\`◉ 𝗦ɪᴢᴇ :\` ${fileInfo.size}
\`◉ 𝗟ɪɴᴋ :\` ${gdriveQuery}

⏳ *Ｄᴏᴡɴʟᴏᴀᴅɪɴɢ Ｆɪʟᴇ...*

> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

    await socket.sendMessage(sender, {
        text: gdriveDesc,
        contextInfo: {
            mentionedJid: [sender],
            forwardingScore: 999,
            isForwarded: true,
            forwardedNewsletterMessageInfo: {
                newsletterJid: '120363425542933159@newsletter',
                newsletterName: "Ｎᴇxᴜꜱ Ｍᴅ 📌",
                serverMessageId: 143,
            },
        }
    }, { quoted: dtzminibot });

    await socket.sendMessage(sender, {
        document: { 
            url: fileInfo.downloadLink
        },
        mimetype: fileInfo.mimeType || 'application/octet-stream',
        fileName: fileInfo.name,
        caption: `✅ *Ｄᴏᴡɴʟᴏᴀᴅ Ｃᴏᴍᴘʟᴇᴛᴇᴅ*\n\n◉ ${fileInfo.name}\n◉ ${fileInfo.size}\n\n> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*\n> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`
    }, { quoted: dtzminibot });

    await socket.sendMessage(sender, {
        react: {
            text: '✅',
            key: msg.key
        }
    });

    break;

// ------------------------------------------------- MEDIAFIRE --------------------------------------------------
case 'mediafire':
case 'mf':
case 'mfdl':
    await socket.sendMessage(sender, {
        react: {
            text: '📥',
            key: msg.key
        }
    });

    const mfQ = msg.message?.conversation || 
                msg.message?.extendedTextMessage?.text || 
                msg.message?.imageMessage?.caption || 
                msg.message?.videoMessage?.caption || '';
    
    const mfQuery = mfQ.split(' ').slice(1).join(' ').trim();

    if (!mfQuery) {
        return await socket.sendMessage(sender, {
            text: '\`🚫 ᴘʟᴇᴀꜱᴇ ꜱᴇɴᴅ ᴀ ᴍᴇᴅɪᴀꜰɪʀᴇ ʟɪɴᴋ\`'
        }, { quoted: dtzminibot });
    }

    await socket.sendMessage(sender, {
        text: '*⏳ Fᴇᴛᴄʜɪɴɢ Mᴇᴅɪᴀꜰɪʀᴇ Fɪʟᴇ Iɴꜰᴏ...*'
    }, { quoted: dtzminibot });

    const mfApi = `https://tharuzz-ofc-apis.vercel.app/api/download/mediafire?url=${encodeURIComponent(mfQuery)}`;
    const mfResponse = await axios.get(mfApi);
    const mfData = mfResponse.data;

    if (!mfData.success || !mfData.result) {
        return await socket.sendMessage(sender, {
            text: '\`❌ ꜰᴀɪʟᴇᴅ ᴛᴏ ꜰᴇᴛᴄʜ ᴍᴇᴅɪᴀꜰɪʀᴇ ꜰɪʟᴇ\`'
        }, { quoted: dtzminibot });
    }

    const mfResult = mfData.result;
    const mfTitle = mfResult.title || mfResult.filename;
    const mfFilename = mfResult.filename;
    const mfFileSize = mfResult.size;
    const mfDownloadUrl = mfResult.url;

    const mfCaption = `_◉ 𝗠ᴇᴅɪᴀꜰɪʀᴇ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_

\`◉ ꜰɪʟᴇ ɴᴀᴍᴇ :\` ${mfFilename}
\`◉ ꜱɪᴢᴇ :\` ${mfFileSize}
\`◉ ꜰʀᴏᴍ :\` ${mfResult.from}
\`◉ ᴅᴀᴛᴇ :\` ${mfResult.date}
\`◉ ᴛɪᴍᴇ :\` ${mfResult.time}

> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

    await socket.sendMessage(sender, {
        document: { url: mfDownloadUrl },
        fileName: mfFilename,
        mimetype: 'application/octet-stream',
        caption: mfCaption
    }, { quoted: dtzminibot });

    await socket.sendMessage(sender, {
        react: {
            text: '✅',
            key: msg.key
        }
    });

    break;

// ------------------------------------------------- FBDL --------------------------------------------------
                case 'fbdl':
                case 'facebook':
                case 'fb': {
                    const axios = require('axios');
                    const q = msg.message?.conversation ||
                        msg.message?.extendedTextMessage?.text ||
                        msg.message?.buttonsResponseMessage?.selectedButtonId || '';

                    const link = q.replace(/^[.\/!]facebook(dl)?\s*/i, '').trim();

                    if (!link) return await socket.sendMessage(sender, {
                        text: '\`📃 Uꜱᴀɢᴇ :\` .ꜰʙ <ʟɪɴᴋ>'
                    }, {
                        quoted: dtzminibot
                    });
                    if (!link.includes('facebook.com')) return await socket.sendMessage(sender, {
                        text: '\`❌ ɪɴᴠᴀʟɪᴅ ꜰᴀᴄᴇʙᴏᴏᴋ ʟɪɴᴋ.\`'
                    }, {
                        quoted: dtzminibot
                    });

                    try {
                        const apiUrl = `https://supunmd.vercel.app/api/download/fbdl?url=${encodeURIComponent(link)}&apikey=supunmd-9m1c6damde1ri93d32kvi`;
                        const {
                            data
                        } = await axios.get(apiUrl);
                        if (!data.data) return await socket.sendMessage(sender, {
                            text: '\`❌ ɴᴏ ʀᴇꜱᴜʟᴛꜱ ꜰᴏᴜɴᴅ\`'
                        });

                        const fb = data.data;
                        const desc = `_◉ 𝗙ᴀᴄᴇʙᴏᴏᴋ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_

╭━━━━━━━━━━━━━━━━━●◌
│ \`■ 𝗧ɪᴛʟᴇ :\` ${fb.title}
│ \`■ 𝗟ɪɴᴋ :\` ${link}
│ \`■ 𝗧ʏᴘᴇꜱ :\` 𝗔ɴʏ.
╰━━━━━━━━━━━━━━━━━●◌

> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                        const buttons = [{
                                buttonId: `${config.PREFIX}fbnormal ${link}`,
                                buttonText: {
                                    displayText: '© Ｖɪᴅᴇᴏ Ｔʏᴘᴇ'
                                },
                                type: 1
                            },
                            {
                                buttonId: `${config.PREFIX}fbvnote ${link}`,
                                buttonText: {
                                    displayText: '© Ｖɪᴅᴇᴏ Ｎᴏᴛᴇ Ｔʏᴘᴇ'
                                },
                                type: 1
                            },
                            {
                                buttonId: `${config.PREFIX}fbdocument ${link}`,
                                buttonText: {
                                    displayText: '© Ｄᴏᴄᴜᴍᴇɴᴛ Ｔʏᴘᴇ'
                                },
                                type: 1
                            }
                        ];

                        await socket.sendMessage(sender, {
                            buttons,
                            headerType: 1,
                            viewOnce: true,
                            caption: desc,
                            image: {
                                url: fb.thumbnail
                            },
                            contextInfo: {
                                mentionedJid: [sender],
                                forwardingScore: 999,
                                isForwarded: true,
                                forwardedNewsletterMessageInfo: {
                                    newsletterJid: '120363425542933159@newsletter',
                                    newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                    serverMessageId: 143
                                }
                            }
                        }, {
                            quoted: dtzminibot
                        });

                    } catch (e) {
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ᴠɪᴅᴇᴏ ʀᴇQᴜꜱᴛ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }

                case 'fbvnote': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const {
                            data: apiData
                        } = await axios.get(
                            `https://apis.prexzyvilla.site/download/facebookv2?url=${encodeURIComponent(videoUrl)}`, {
                                timeout: 15000
                            }
                        );

                        if (!apiData?.data?.hd?.length) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });
                            break;
                        }

                        const firstLink = apiData.data.download_links[0];

                        await socket.sendMessage(sender, {
                            video: {
                                url: firstLink.url
                            },
                            mimetype: 'video/mp4',
                            ptv: true,
                            fileName: `${apiData.data.title || 'Fᴀᴄᴇʙᴏᴏᴋ Vɪᴅᴇᴏ'}.mp4`
                        }, {
                            quoted: dtzminibot
                        });

                    } catch (err) {
                        console.error('FBVNote Error:', err.message);
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴘʀᴏᴠᴇꜱꜱɪɴɢ ᴠɪᴅᴇᴏ ɴᴏᴛᴇ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }


                case 'fbdocument': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const {
                            data: apiData
                        } = await axios.get(
                            `https://apis.prexzyvilla.site/download/facebookv2?url=${encodeURIComponent(videoUrl)}`, {
                                timeout: 15000
                            }
                        );

                        if (!apiData?.data?.download_links?.length) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });
                            break;
                        }

                        const firstLink = apiData.data.download_links[0];

                        await socket.sendMessage(sender, {
                            document: {
                                url: firstLink.url
                            },
                            mimetype: 'video/mp4',
                            fileName: `Fʙ Vɪᴅᴇᴏ - ${apiData.data.title || 'Vɪᴅᴇᴏ'}.mp4`
                        }, {
                            quoted: dtzminibot
                        });

                    } catch (err) {
                        console.error('FBDocument Error:', err.message);
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ᴀꜱ ᴅᴏᴄᴜᴍᴇɴᴛ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }

                case 'fbnormal': {
                    const axios = require('axios');
                    const videoUrl = args[0] || q;

                    try {
                        const {
                            data: apiData
                        } = await axios.get(
                            `https://apis.prexzyvilla.site/download/facebookv2?url=${encodeURIComponent(videoUrl)}`, {
                                timeout: 15000
                            }
                        );

                        if (!apiData?.data?.download_links?.length) {
                            await socket.sendMessage(sender, {
                                text: '\`❌ ᴠɪᴅᴇᴏ ɴᴏᴛ ꜰᴏᴜɴᴅ\`'
                            }, {
                                quoted: dtzminibot
                            });
                            break;
                        }

                        const firstLink = apiData.data.download_links[0];

                        const titleCaption = apiData.data.title ? `${apiData.data.title}\n\n` : '';

                        await socket.sendMessage(sender, {
                            video: {
                                url: firstLink.url
                            },
                            mimetype: 'video/mp4',
                            fileName: `Fᴀᴄᴇʙᴏᴏᴋ Vɪᴅᴇᴏ - ${apiData.data.title || 'Vɪᴅᴇᴏ'}.mp4`
                        }, {
                            quoted: dtzminibot
                        });

                    } catch (err) {
                        console.error('FBNormal Error:', err.message);
                        await socket.sendMessage(sender, {
                            text: '\`❌ ᴇʀʀᴏʀ ᴅᴏᴡʟᴏᴀᴅɪɴɢ ᴠɪᴅᴇᴏ\`'
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }

// ------------------------------------------------- APKDL --------------------------------------------------
case 'apk':
case 'apkdown':
case 'apkdl': {
    try {
        const axios = require('axios');

        const q =
            msg.message?.conversation ||
            msg.message?.extendedTextMessage?.text ||
            msg.message?.imageMessage?.caption ||
            msg.message?.videoMessage?.caption || '';

        const query = q.replace(/^\.(?:apkdl2|apkdown|getapk)\s+/i, '').trim();

        if (!query) {
            return await socket.sendMessage(sender, {
                text: '\`❌ ᴘʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴀɴ ᴀᴘᴘ ɴᴀᴍᴇ\`'
            }, { quoted: dtzminibot });
        }

        
        const apiUrl = `https://saviya-kolla-api.koyeb.app/download/apk?q=${encodeURIComponent(query)}`;
        const { data } = await axios.get(apiUrl);

        if (!data || !data.result) {
            return await socket.sendMessage(sender, {
                text: '\`❌ Aᴘᴋ ɴᴏᴛ ꜰᴏᴜɴᴅ ᴘʟᴇᴀꜱᴇ ᴛʀʏ ᴀɴᴏᴛʜᴇʀ ᴀᴘᴘ ɴᴀᴍᴇ.\`'
            }, { quoted: dtzminibot });
        }

        const result = data.result;

        const caption = `_◉ 𝗔ᴘᴋ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_

╭━━━━━━━━━━━━━━━━━●◌
│ \`■ *𝗔ᴘᴘ :\` ${result.name}
│ \`■ *𝗣ᴀᴄᴋᴀɢᴇ :\` ${result.package}
│ \`■ *𝗦ɪᴢᴇ :\` ${result.size}
│ \`■ *𝗥ᴀᴛɪɴɢ :\` ${result.rating || 'N/A'}
╰━━━━━━━━━━━━━━━━━●◌

> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

        const buttons = [
            {
                buttonId: `${config.PREFIX}apkdlbtn ${result.name}`,
                buttonText: { displayText: 'Ｄᴏᴡɴʟᴏᴀᴅ Ａᴘᴋ ⬇️' },
                type: 1
            }
        ];

        await socket.sendMessage(sender, {
            image: { url: result.icon },
            caption: caption,
            headerType: 1,
            buttons,
            viewOnce: true,
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: dtzminibot });

    } catch (e) {
        console.error(e);
        await socket.sendMessage(sender, { text: '\`❌ ꜱᴏᴍᴇᴛʜɪɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ ᴘʟᴇᴀꜱᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ\`' }, { quoted: dtzminibot });
    }
    break;
}

case 'apkdlbtn': {
    try {
        const apkName = args.join(' ');
        if (!apkName) return;

        const apiUrl = `https://saviya-kolla-api.koyeb.app/download/apk?q=${encodeURIComponent(apkName)}`;
        const { data } = await axios.get(apiUrl);
        const result = data.result;

        if (!result || !result.dllink) {
            return await socket.sendMessage(sender, { text: '\`❌ ᴀᴘᴋ ɴᴏᴛ ᴀᴠᴀɪʟᴀʙʟᴇ\`' }, { quoted: dtzminibot });
        }

        await socket.sendMessage(sender, {
            document: { url: result.dllink },
            mimetype: 'application/vnd.android.package-archive',
            fileName: `${result.name}.apk`,
            caption: `\`✅ Dᴏᴡɴʟᴏᴀᴅ ᴅᴏɴᴇ :\` *${result.name}*...`
        }, { quoted: dtzminibot });

    } catch (e) {
        console.error(e);
        await socket.sendMessage(sender, { text: '\❌ ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ\`' }, { quoted: dtzminibot });
    }
    break;
}

// ------------------------------------------------- TIKTOK --------------------------------------------------
case 'tiktok':
case 'tt': {
    try {
        const axios = require('axios');

        const q =
            msg.message?.conversation ||
            msg.message?.extendedTextMessage?.text ||
            msg.message?.imageMessage?.caption ||
            msg.message?.videoMessage?.caption || '';

        const url = q.replace(/^\.(tiktok|tt)\s+/i, '').trim();
        if (!url) return;

        const api = `https://movanest.xyz/v2/tiktok?url=${encodeURIComponent(url)}`;
        const res = await axios.get(api).then(r => r.data).catch(() => null);
        if (!res || !res.status || !res.results?.no_watermark) return;

        const data = res.results;

        const caption = `_◉ 𝗧ɪᴋᴛᴏᴋ 𝗗ᴏᴡɴʟᴏᴀᴅ ◉_

╭━━━━━━━━━━━━━━━━━━━━●◌
│ \`■ 𝗧ɪᴛʟᴇ :\` ${data.title || 'N/ᴀ'}
│ \`■ 𝗔ᴜᴛʜᴏʀ :\` ${data.author || '𝗨ɴᴋɴᴏᴡɴ'}
╰━━━━━━━━━━━━━━━━━━━━●◌
> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

        const buttons = [
            { buttonId: `.ttdoc ${url}`, buttonText: { displayText: '© Ｄᴏᴄᴜᴋᴇɴᴛ Ｔʏᴘᴇ' }, type: 1 },
            { buttonId: `.ttvideo ${url}`, buttonText: { displayText: '© Ｖɪᴅᴇᴏ Ｔʏᴘᴇ' }, type: 1 },
            { buttonId: `.ttnote ${url}`, buttonText: { displayText: '© Ｖɪᴅᴇᴏ Ｎᴏᴛᴇ Ｔʏᴘᴇ' }, type: 1 }
        ];

        await socket.sendMessage(sender, {
            image: { url: data.cover || data.origin_cover },
            caption,
            buttons,
            headerType: 1,
            viewOnce: true
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴛɪᴋᴛᴏᴋ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
    break;
}

case 'ttdoc': {
    try {
        const axios = require('axios');
        const url = args[0];
        if (!url) return;

        const api = `https://movanest.xyz/v2/tiktok?url=${encodeURIComponent(url)}`;
        const res = await axios.get(api).then(r => r.data).catch(() => null);
        if (!res || !res.status || !res.results?.no_watermark) return;

        await socket.sendMessage(sender, {
            document: { url: res.results.no_watermark },
            mimetype: 'video/mp4',
            fileName: `${res.results.title || 'tiktok'}.mp4`
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴅᴏᴄᴜᴍᴇɴᴛ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
    break;
}

case 'ttvideo': {
    try {
        const axios = require('axios');
        const url = args[0];
        if (!url) return;

        const api = `https://movanest.xyz/v2/tiktok?url=${encodeURIComponent(url)}`;
        const res = await axios.get(api).then(r => r.data).catch(() => null);
        if (!res || !res.status || !res.results?.no_watermark) return;

        await socket.sendMessage(sender, {
            video: { url: res.results.no_watermark },
            mimetype: 'video/mp4'
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴠɪᴅᴇᴏ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
    break;
}

case 'ttnote': {
    try {
        const axios = require('axios');
        const url = args[0];
        if (!url) return;

        const api = `https://movanest.xyz/v2/tiktok?url=${encodeURIComponent(url)}`;
        const res = await axios.get(api).then(r => r.data).catch(() => null);
        if (!res || !res.status || !res.results?.no_watermark) return;

        await socket.sendMessage(sender, {
            video: { url: res.results.no_watermark },
            mimetype: 'video/mp4',
            ptv: true
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴠɪᴅᴇᴏ ɴᴏᴛᴇ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
    break;
}

// ------------------------------------------------- XNXX --------------------------------------------------                   
case 'xnxx': {
    try {
        const axios = require('axios');

        const q =
            msg.message?.conversation ||
            msg.message?.extendedTextMessage?.text ||
            msg.message?.imageMessage?.caption ||
            msg.message?.videoMessage?.caption || '';

        if (!q.trim()) {
            return await socket.sendMessage(
                sender,
                { text: '\`❌ ɴᴇᴇᴅ ᴛɪᴛʟᴇ ᴏʀ ᴜʀʟ ᴏʀ ᴋᴇʏᴡᴏʀᴅ\`' },
                { quoted: dtzminibot }
            );
        }

        let videoUrl = q;

        if (!q.includes('xnxx.com')) {
            const searchApi = `https://apis.prexzyvilla.site/nsfw/xnxx-search?query=${encodeURIComponent(q)}`;
            const search = await axios.get(searchApi, { timeout: 15000 })
                .then(r => r.data)
                .catch(() => null);

            if (!search || !search.status || !search.videos?.length) {
                return await socket.sendMessage(
                    sender,
                    { text: '\`❌ Nᴏ ʀᴇꜱᴜʟᴛꜱ ꜰᴏᴜɴᴅ\`' },
                    { quoted: msg }
                );
            }

            videoUrl = search.videos[0].link;
        }

        const dlApi = `https://apis.prexzyvilla.site/nsfw/xnxx-dl?url=${encodeURIComponent(videoUrl)}`;
        const data = await axios.get(dlApi, { timeout: 15000 })
            .then(r => r.data)
            .catch(() => null);

        if (!data || data.status !== true) {
            return await socket.sendMessage(
                sender,
                { text: '\`❌ ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟᴇᴅ' },
                { quoted: msg }
            );
        }

        const desc = `_🔞◉ 𝗫ɴxx 𝗗ᴏᴡɴʟᴏᴀᴅ ◉🔞_

╭━━━━━━━━━━━━━━━━━━━━●◌
│ \`■ 𝗧ɪᴛʟᴇ :\` ${data.title}
│ \`■ 𝗗ᴜʀᴀᴛɪᴏɴ :\` ${data.duration}
│ \`■ 𝗤ᴜᴀʟɪᴛʏ :\` Hᴅ
│ \`■ 𝗗ᴇꜱᴄʀɪᴘᴛɪᴏɴ :\` ${data.info}
╰━━━━━━━━━━━━━━━━━━━━●◌
> *© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

        const buttons = [
            {
                buttonId: `${config.PREFIX}xn ${data.url}`,
                buttonText: { displayText: '© Ｖɪᴅᴇᴏ Ｔʏᴘᴇ' },
                type: 1
            },
            {
                buttonId: `${config.PREFIX}xnvnotei ${data.url}`,
                buttonText: { displayText: '© Ｖɪᴅᴇᴏ Ｎᴏᴛᴇ Ｔʏᴘᴇ️' },
                type: 1
            },
            {
                buttonId: `${config.PREFIX}xndoc ${data.url}`,
                buttonText: { displayText: '© Ｄᴏᴄᴜᴍᴇɴᴛ Ｔʏᴘᴇ' },
                type: 1
            }
        ];

        await socket.sendMessage(sender, {
            image: { url: data.image },
            caption: desc,
            headerType: 1,
            buttons,
            viewOnce: true,
            contextInfo: {
                mentionedJid: [sender],
                forwardingScore: 999,
                isForwarded: true,
                forwardedNewsletterMessageInfo: {
                    newsletterJid: '120363425542933159@newsletter',
                    newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                    serverMessageId: 143
                }
            }
        }, { quoted: dtzminibot });

    } catch (e) {
        console.error(e);
        await socket.sendMessage(
            sender,
            { text: '\`❌ ꜱᴏᴍᴇᴛʜɪᴍɢ ᴡᴇɴᴛ ᴡʀᴏɴɢ ᴘʟᴇᴀꜱᴇ ᴛʀʏ ᴀɢᴀɪɴ\`' },
            { quoted: dtzminibot }
        );
    }
    break;
}


case 'xn': {
    try {
        const axios = require('axios');
        const videoUrl = args[0];

        if (!videoUrl) return;

        const apiUrl = `https://apis.prexzyvilla.site/nsfw/xnxx-dl?url=${encodeURIComponent(videoUrl)}`;
        const data = await axios.get(apiUrl).then(r => r.data);

        const video = data?.files?.high;
        
        if (!video) throw 'Nᴏ ᴠɪᴅᴇᴏ';

        await socket.sendMessage(sender, {
            video: { url: video },
            mimetype: 'video/mp4',
            fileName: `${data.title}.mp4`
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴠɪᴅᴇᴏ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
}
break;

case 'xnvnotei': {
    try {
        const axios = require('axios');
        const videoUrl = args[0];

        const apiUrl = `https://apis.prexzyvilla.site/nsfw/xnxx-dl?url=${encodeURIComponent(videoUrl)}`;
        const data = await axios.get(apiUrl).then(r => r.data);

        const video = data?.files?.low;

        await socket.sendMessage(sender, {
            video: { url: video },
            mimetype: 'video/mp4',
            ptv: true
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴠɪᴅᴇᴏ ɴᴏᴛᴇ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
}
break;

case 'xndoc': {
    try {
        const axios = require('axios');
        const videoUrl = args[0];

        const apiUrl = `https://apis.prexzyvilla.site/nsfw/xnxx-dl?url=${encodeURIComponent(videoUrl)}`;
        const data = await axios.get(apiUrl).then(r => r.data);

        const video = data?.files?.high;

        await socket.sendMessage(sender, {
            document: { url: video },
            mimetype: 'video/mp4',
            fileName: `${data.title}.mp4`
        }, { quoted: dtzminibot });

    } catch {
        await socket.sendMessage(sender, { text: '\`❌ ᴅᴏᴄᴜᴍᴇɴᴛ ᴇʀʀᴏʀ\`' }, { quoted: dtzminibot });
    }
}
break;



//==========




// --------------------------- PING ---------------------------
case 'ping': {
    const os = require("os")
    const start = Date.now();

    const loading = await socket.sendMessage(sender, {
        text: "*ɴᴇxᴜꜱ- ᴍᴅ - ᴍɪɴɪ ꜱɪɢɴᴀʟ 👨‍🔧💚🛰️*"
    }, { quoted: dtzminibot });

    const stages = ["◍○○○○", "◍◍○○○", "◍◍◍○○", "◍◍◍◍○", "◍◍◍◍◍"];
    for (let stage of stages) {
        await socket.sendMessage(sender, { text: stage, edit: loading.key });
        await new Promise(r => setTimeout(r, 250));
    }

    const end = Date.now();
    const ping = end - start;

    await socket.sendMessage(sender, {
        text: `🧩 Ｐɪɴɢ  ▻  \`2ᴍꜱ\`\n\n ʙᴏᴛ ɪꜱ ᴀᴄᴛɪᴠᴇ ᴛᴏ ꜱɪɢɴᴀʟ 💚⚡`,
        edit: loading.key
    });

    break;
}

// --------------------------- OWNER ---------------------------
case 'owner': {
    const ownerNumber = '+94774571418';
    const ownerName = 'Ｎᴇxᴜꜱ Ｍᴅ Ｄᴇᴠ';
    const organization = '*Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｍᴀɪɴ Ｏᴡɴᴇʀ*';

    const vcard = 'BEGIN:VCARD\n' +
                  'VERSION:3.0\n' +
                  `FN:${ownerName}\n` +
                  `ORG:${organization};\n` +
                  `TEL;type=CELL;type=VOICE;waid=${ownerNumber.replace('+', '')}:${ownerNumber}\n` +
                  'END:VCARD';

    try {
        const sent = await socket.sendMessage(sender, {
            contacts: {
                displayName: ownerName,
                contacts: [{ vcard }]
            }
        });
      
        await socket.sendMessage(sender, {
            text: `*💚 Ｎᴇxᴜꜱ Ｍᴅ Ｍᴀɪɴ Ｏᴡɴᴇʀ*\n\n👨‍🔧 Nᴀᴍᴇ : ${ownerName}\n💭 Nᴜᴍʙᴇʀ ➥ ${ownerNumber}\n\n> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`,
            contextInfo: {
                mentionedJid: [`${ownerNumber.replace('+', '')}@s.whatsapp.net`],
                quotedMessageId: sent.key.id
            }
        }, { quoted: msg });

    } catch (err) {
        console.error('❌ Owner command error:', err.message);
        await socket.sendMessage(sender, {
            text: '\`❌ Eʀʀᴏʀ ꜱᴇɴᴅɪɴɢ ᴏᴡɴᴇʀ ᴄᴏɴᴛᴀᴄᴛ.\`'
        }, { quoted: msg });
    }
        
  break;
}

// --------------------------- GETDP ---------------------------
case "getdp":
case "dp":
case "profile": {
    try {
        const user = m.quoted 
            ? m.quoted.sender 
            : m.mentionedJid[0] 
            ? m.mentionedJid[0] 
            : m.sender;

        const loading = await socket.sendMessage(sender, {
            text: "*ɢᴇᴛᴛɪɴɢ ᴘʀᴏꜰɪʟᴇ ᴘɪᴄ... 🖼️🖤*"
        }, { quoted: msg });

        const stages = ["◍○○○○", "◍◍○○○", "◍◍◍○○", "◍◍◍◍○", "◍◍◍◍◍"];
        for (let stage of stages) {
            await socket.sendMessage(sender, { text: stage, edit: loading.key });
            await new Promise(r => setTimeout(r, 250));
        }

        const pp = await socket.profilePictureUrl(user, "image").catch(() => null);

        if (!pp) {
            return await socket.sendMessage(sender, {
                text: "\`❌ ɴᴏ ᴘʀᴏꜰɪʟᴇ ᴘɪᴄ ꜰᴏᴜɴᴅ!\`",
                edit: loading.key
            });
        }

        await socket.sendMessage(sender, {
            image: { url: pp },
            caption: `✨ *ᴘʀᴏꜰɪʟᴇ ᴘɪᴄᴛᴜʀᴇ ꜱᴀᴠᴇᴅ*\n👤 @${user.split("@")[0]}\n\n> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`
        }, {
            quoted: msg,
            mentions: [user]
        });

    } catch (err) {
        console.log(err);
        await socket.sendMessage(sender, {
            text: "\`❌ ᴇʀʀᴏʀ ɢᴇᴛᴛɪɴɢ ᴅᴘ!\`"
        }, { quoted: msg });
    }
}
break;

// --------------------------- ALIVE ---------------------------
case 'alive': {
    const startTime = socketCreationTime.get(number) || Date.now();
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = Math.floor(uptime % 60);

    const captionText = `
❲ ʜɪ ɪ ᴀᴍ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ ᴠᴇʀꜱɪᴏɴ 1 ❳

║▻ Ｉ ＡＭ ＡＬＩＶＥ 👨‍🔧🔥 ◅║

╭────◅●●▻────➣
\`⭓ ʙᴏᴛ ᴜᴘ ᴛɪᴍᴇ ➟\` ${hours}ʜ ${minutes}ᴍ ${seconds}ꜱ 🫧
\`⭓ ʙᴏᴛᴀᴄᴛɪᴠᴇ ᴄᴏᴜɴᴛ ➟\` ${activeSockets.size} ✨
\`⭓ ᴍɪɴɪ ᴠᴇʀꜱɪᴏɴ ➟\` 1.0.0 ᴠ 🧬
\`⭓ ᴅᴇᴘʟᴏʏ ᴘʟᴀᴛꜰʀᴏᴍ ➟\` Hᴇʀᴏᴋᴜ ❲ ꜰʀᴇᴇ ❳ ⚙️
\`⭓ ᴍɪɴɪ ʙᴏᴛ ᴏᴡɴᴇʀ ➟\` .ᴏᴡɴᴇʀ 🪩
╰────◅●●▻────➢


*➟ Tʜɪꜱ ɪꜱ ᴛʜᴇ ʀᴇꜱᴜʟᴛ ᴏꜰ ᴏᴜʀ ᴛᴇᴀᴍꜱ ʜᴀʀᴅ ᴡᴏʀᴋ ᴛʜᴇʀᴇꜰᴏʀᴇ , ᴘʟᴇᴀꜱᴇ ʀᴇꜱᴘᴇᴄᴛ ᴛʏᴇ ꜱᴏᴜʀᴄᴇ ᴀɴᴅ ᴀᴠᴏɪʀ ᴜɴᴀᴜᴛʜᴏʀɪᴢᴇᴅ ᴇᴅɪᴛꜱ ◅*

*💡 Iꜰ ʏᴏᴜ ɴᴇᴇᴅ ʜᴇʟᴘ ʀᴇɢᴀʀᴅɪɴɢ ᴛʜᴇ ʙᴏᴛ , ᴛʏᴘᴇ :* .ʜᴇʟᴘ

*🌐 ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ ᴡᴇʙ ꜱɪᴛᴇ :*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ɴᴇxᴜꜱ ᴍᴅ ʙᴇᴛᴀ ᴡᴀ ʙᴏᴛ 1.0.0 ᴘʀᴏ*
> *● ᴡᴀʙᴏᴛ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ ●*

> 🌐 Wᴇʙ : Cᴏᴍɪɴɢ Sᴏᴏɴ
> 🎬 Tᴜᴛᴏʀɪᴀʟ : Cᴏᴍɪɴɢ Sᴏᴏɴ

> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*
`;

    const templateButtons = [
        {
            buttonId: `${config.PREFIX}menu`,
            buttonText: { displayText: '❲ 𝘔𝘌𝘕𝘜  ❤️ ❳' },
            type: 1,
        },
        {
            buttonId: `${config.PREFIX}owner`,
            buttonText: { displayText: ' ❲ 𝘖𝘞𝘕𝘌𝘙  ❤️ ❳' },
            type: 1,
        },
        {
            buttonId: 'action',
            buttonText: {
                displayText: ' ◅ ❤️👨‍🔧 ᴍᴇɴᴜ ᴏᴘᴄᴛɪᴏɴꜱ ▻'
            },
            type: 4,
            nativeFlowInfo: {
                name: 'single_select',
                paramsJson: JSON.stringify({
                    title: 'Tᴀʙ Aɴᴅ Sᴇʟᴇᴄᴛɪᴏɴ ❕',
                    sections: [
                        {
                            title: `ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ 👨‍🔧⚡`,
                            highlight_label: '',
                            rows: [
                                {
                                    title: '❲ Ｍᴇɴᴜ  ❤️ ❳',
                                    description: 'ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ᴄᴍᴅ ʟɪꜱᴛ 👨‍🔧⚡',
                                    id: `${config.PREFIX}menu`,
                                },
                                {
                                    title: '❲ Ｏᴡɴᴇʀ ❤️ ❳',
                                    description: 'ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ 👨‍🔧⚡',
                                    id: `${config.PREFIX}owner`,
                                },
                            ],
                        },
                    ],
                }),
            },
        }
    ];

    await socket.sendMessage(sender, {
        buttons: templateButtons,
        headerType: 1,
        viewOnce: true,
        image: { url: "https://d.uguu.se/JcHekQtn.jpg" },
        caption: `${captionText}`,
    }, { quoted: msg });

    break;
}

// --------------------------- SYSTEM ---------------------------
case 'system': {
	
    const startTime = socketCreationTime.get(number) || Date.now();
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = Math.floor(uptime % 60);

    
const captionText = `
║▻ Ｎᴇxᴜꜱ Ｍᴅ Ｓʏꜱᴛᴇᴍ Ｉɴꜰᴏ 👨‍🔧🖤◅║

╭────◅●⭓●▻────➣
\`⭓ ʙᴏᴛ ᴜᴘ ᴛɪᴍᴇ ➟\` ${hours}ʜ ${minutes}ᴍ ${seconds}ꜱ 🫧
\`⭓ ʙᴏᴛᴀᴄᴛɪᴠᴇ ᴄᴏᴜɴᴛ ➟\` ${activeSockets.size} ✨
\`⭓ ᴍɪɴɪ ᴠᴇʀꜱɪᴏɴ ➟\` 1.0.0 ᴠ 👨‍🔧
\`⭓ ʀᴀᴍ ᴜꜱᴇɢᴇ ➟\` ɴᴏ ɴᴇᴇᴅ 🧬
\`⭓ ᴅᴇᴘʟᴏʏ ᴘʟᴀᴛꜰʀᴏᴍ ➟\` Hᴇʀᴏᴋᴜ ❲ ꜰʀᴇᴇ ❳ 💻
\`⭓ ᴍɪɴɪ ʙᴏᴛ ᴏᴡɴᴇʀ ➟\` ᴏᴡɴᴇʀ ʙᴜᴛᴛᴏɴ ⚙️
╰────◅●⭓●▻────➢

> *© ɴᴇxᴜꜱ ᴍᴅ ʙᴇᴛᴀ ᴡᴀ ʙᴏᴛ 1.0.0 ᴘʀᴏ*
> *● ᴡᴀʙᴏᴛ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ ●*

> 🌐 Wᴇʙ : Cᴏᴍɪɴɢ Sᴏᴏɴ
> 🎬 Tᴜᴛᴏʀɪᴀʟ : Cᴏᴍɪɴɢ Sᴏᴏɴ

> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*
`;
	
    const templateButtons = [
        {
            buttonId: `${config.PREFIX}ping`,
            buttonText: { displayText: '💚🔥 ɴᴇxᴜꜱ ᴍᴅ ᴘɪɴɢ ꜱɪɢɴᴀʟ' },
            type: 1,
        },
        {
            buttonId: `${config.PREFIX}menu`,
            buttonText: { displayText: '💚🔥 ɴᴇxᴜꜱ ᴍᴅ ᴍᴇɴᴜ ʟɪꜱᴛ' },
            type: 1,
        },
        {
            buttonId: `${config.PREFIX}owner`,
            buttonText: { displayText: '💚🔥 Qɴᴇxᴜꜱ ᴍᴅ ᴄᴏɴᴛᴀᴄᴛ ᴏᴡɴᴇʀ' },
            type: 1
        }
    ];
    
    await socket.sendMessage(sender, {
        image: { url: "https://d.uguu.se/JcHekQtn.jpg" },
        caption: `${captionText}`,
        buttons: templateButtons,
        headerType: 1
    }, { quoted: msg });

    break;
}

// --------------------------- FANCY ---------------------------
case 'fancy': {
  const axios = require("axios");

  const q =
    msg.message?.conversation ||
    msg.message?.extendedTextMessage?.text ||
    msg.message?.imageMessage?.caption ||
    msg.message?.videoMessage?.caption || '';

  const text = q.trim().replace(/^.fancy\s+/i, "");

  if (!text) {
    return await socket.sendMessage(sender, {
      text: "❎ *Pʟᴇᴀꜱᴇ ᴘʀᴏᴠɪᴅᴇ ᴛᴇxᴛ ᴛᴏ ᴄᴏɴᴠᴇʀᴛ ɪɴᴛᴏ ꜰᴀɴᴄʏ ꜰᴏɴᴛꜱ.*\n\n📌 *Exᴀᴍᴘʟᴇ :* `.ꜰᴀɴᴄʏ Qᴜᴇᴇɴ`"
    });
  }

  try {
    const apiUrl = `https://appi.srihub.store/tools/styletext?text=${encodeURIComponent(text)}&apikey=dew_GzzWuBRB9nzovpLWdPUNuAFtY1rDCYp7pYR5zmDk`;
    const response = await axios.get(apiUrl);

    if (!response.data.status || !response.data.result) {
      return await socket.sendMessage(sender, {
        text: "❌ *Eʀʀᴏʀ ꜰᴇᴛᴄʜɪɴɢ ꜰᴏɴᴛꜱ ꜰʀᴏᴍ ᴀᴘɪ , ᴘʟᴇᴀꜱᴇ ᴛʀʏ ᴀɢᴀɪɴ ʟᴀᴛᴇʀ.*"
      });
    }

    const fontList = response.data.result
      .map(font => `*${font.name}:*\n${font.result}`)
      .join("\n\n");

    const finalMessage = `\`🎨 Fᴀɴᴄʏ Fᴏɴᴛꜱ Cᴏɴᴠᴇʀᴛᴇʀ\`\n\n${fontList}\n\n> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

    await socket.sendMessage(sender, {
      text: `${finalMessage}`
    }, { quoted: msg });

  } catch (err) {
    console.error("Fancy Font Error:", err);
    await socket.sendMessage(sender, {
      text: "⚠️ *Aɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴄᴏɴᴠᴇʀᴛɪɴɢ ᴛɪ ꜰᴀɴᴄʏ ꜰᴏɴᴛꜱ..*"
    });
  }

  break;
}

// --------------------------- PAIR ---------------------------
case 'pair': {
    const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    const number = q.replace(/^[.\/!]pair\s*/i, '').trim();

    if (!number) {
        return await socket.sendMessage(sender, {
            text: '*📌 Uꜱᴀɢᴇ:* .ᴘᴀɪʀ +9476919XXXX'
        }, { quoted: msg });
    }

    try {
        const url = `https://queen-minuu-md-production.up.railway.app/code?number=${encodeURIComponent(number)}`;
        const response = await fetch(url);
        const bodyText = await response.text();

        console.log("🌐 API Response:", bodyText);

        let result;
        try {
            result = JSON.parse(bodyText);
        } catch (e) {
            console.error("❌ JSON Parse Error:", e);
            return await socket.sendMessage(sender, {
                text: '❌ Iɴᴠᴀʟɪᴅ ʀᴇꜱᴘᴏɴꜱᴇ ꜰʀᴏᴍ ꜱᴇʀᴠᴇʀ ᴘʟᴇᴀꜱᴇ ᴄᴏɴᴛᴀᴄᴛ ꜱᴜᴘᴘᴏʀᴛ.'
            }, { quoted: msg });
        }

        if (!result || !result.code) {
            return await socket.sendMessage(sender, {
                text: '❌ Fᴀɪᴇʟᴅ ᴛᴏ ʀᴇᴛʀɪᴇᴠᴇ ᴘᴀɪʀɪɴɢ ᴄᴏᴅᴇ. ᴘʟᴇᴀꜱᴇ ᴄʜᴇᴄᴋ ᴛʜᴇ ɴᴜᴍʙᴇʀ.'
            }, { quoted: msg });
        }
		await socket.sendMessage(m.chat, { react: { text: '🔑', key: msg.key } });
        await socket.sendMessage(sender, {
            text: `> *Ｐᴀɪʀ Ｃᴏɴɴᴇᴄᴛᴇᴅ* ✅\n\n*🔑 Yᴏᴜʀ ᴘᴀɪʀɪɴɢ ᴄᴏᴅᴇ ɪꜱ:* ${result.code}\n\n
📌Sᴛᴇᴘꜱ -
 Oɴ Yᴏᴜʀ Pʜᴏɴᴇ :
   - Oᴘᴇɴ Wʜᴀᴛꜱᴀᴘᴘ 👨‍🔧
   - Tᴀᴘ 3 ᴅᴏᴛꜱ (⋮) ᴏʀ ɢᴏ ᴛᴏ ꜱᴇᴛᴛɪɴɢ 🧑‍🔧
   - Tᴀᴘ ʟɪɴᴋᴇᴅ ᴀ ᴅᴇᴠɪᴄᴇ 👨‍🔧
   - Tᴀᴘ ʟɪɴᴋ Q ᴅᴇᴠɪᴄᴇ 👨‍🔧 
   - Tᴀᴘ ᴀ ʟɪɴᴋ ᴡɪᴛʜ ᴄᴏᴅᴇ 👨‍🔧
   - Eɴᴛᴇʀ ᴛʜᴇ 8-ᴅɪɢɪᴛ ᴄᴏᴅᴇ ꜱʜᴏᴡɴ ʙʏ ᴛʜᴇ ʙᴏᴛ 👨‍🔧`
        }, { quoted: msg });

        await sleep(2000);

        await socket.sendMessage(sender, {
            text: `${result.code}\n> > ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ`
        }, { quoted: msg });

    } catch (err) {
        console.error("❌ Pair Command Error:", err);
        await socket.sendMessage(sender, {
            text: '❌ Aɴ ᴇʀʀᴏʀ ᴏᴄᴄᴜʀʀᴇᴅ ᴡʜɪʟᴇ ᴘʀᴏᴄᴇꜱꜱɪɴɢ ʏᴏᴜʀ ʀᴇQᴜᴀꜱᴛ..'
        }, { quoted: msg });
    }

    break;
}

// ------------------------------------------------- MENU --------------------------------------------------
case 'menu': {
	const startTime = socketCreationTime.get(number) || Date.now();
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = Math.floor(uptime % 60);

    
    const captionText = `
❲ ʜɪ ɪ ᴀᴍ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ ᴠᴇʀꜱɪᴏɴ 1 ❳

╭────◅●⭓●▻────➣
\`⭓ ʙᴏᴛ ᴜᴘ ᴛɪᴍᴇ ➟\` ${hours}ʜ ${minutes}ᴍ ${seconds}ꜱ 🫧
\`⭓ ʙᴏᴛᴀᴄᴛɪᴠᴇ ᴄᴏᴜɴᴛ ➟\` ${activeSockets.size} ✨
\`⭓ ᴍɪɴɪ ᴠᴇʀꜱɪᴏɴ ➟\` 1.0.0 ᴠ 🧬
\`⭓ ᴅᴇᴘʟᴏʏ ᴘʟᴀᴛꜰʀᴏᴍ ➟\` Hᴇʀᴏᴋᴜ ❲ ꜰʀᴇᴇ ❳ ⚙️
\`⭓ ᴍɪɴɪ ʙᴏᴛ ᴏᴡɴᴇʀ ➟\` .ᴏᴡɴᴇʀ 🪩
╰────◅●⭓●▻────➢

🛡️ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ – A ɴᴇᴡ ᴇʀᴀ ᴏꜰ ᴡʜᴀᴛꜱᴀᴘᴘ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ ⚡

> ᴏᴡɴᴇʀ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ 💥

➟

\`👨‍💻 Aʙᴏᴜᴛ ᴍᴇ\`
𝗜'ᴍ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ʙᴏᴛ , ɴᴇᴡᴜᴘᴅᴀᴛᴇ ᴀɴᴅ ᴇxᴘᴇʀɪᴇɴꜱ.
𝗜 ʙᴜɪʟᴅ ɴᴇxᴜꜱ ᴍᴅ ᴛᴏ ʀᴇᴅᴇꜰɪɴᴇ ᴡʜᴀᴛꜱᴀᴘᴘ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ.

\`🔧 Ｂᴜɪʟᴅ Ｗɪᴛʜ ➟\`

Nᴏᴅᴇ.ᴊꜱ + Jᴀᴠᴀꜱᴄʀɪᴘᴛ

Bᴀɪʟᴇʏꜱ Mᴜʟᴛɪ-Dᴇᴠɪᴄᴇ

Kᴇʏᴅʙ ꜰᴏʀ ꜱᴇꜱꜱɪᴏɴ ᴍᴀɴᴀɢᴇᴍᴇɴᴛ

Aᴜᴛᴏ ᴅᴇᴘʟᴏʏ ᴀɴᴅ ꜰʀᴇᴇ ❕

➟

\`📜 Lᴇɢᴀᴄʏ Pʜʀᴀꜱᴇ ➟\`

“ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ ɪꜱ ɴᴏᴛ ᴊᴜꜱᴛ ᴀ ʙᴏᴛ..ɪᴛꜱ ᴠɪꜱɪᴏɴ ᴄʀᴀꜰᴛᴇᴅ ꜱɪɴᴄᴇ 2025 , ʟᴀᴜɴᴄʜᴇᴅ ɪɴ 2026.”

> *ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

    const templateButtons = [
        {
            buttonId: 'action',
            buttonText: {
                displayText: '❲ 🌐 ᴍᴇɴᴜ ᴏᴘᴄᴛɪᴏɴ ❳'
            },
            type: 4,
            nativeFlowInfo: {
                name: 'single_select',
                paramsJson: JSON.stringify({
                    title: 'Ｎᴇxᴜꜱ Ｍᴅ Ｍᴇɴᴜ 🧑‍🔧',
                    sections: [
                        {
                            title: `🌐 ɴᴇxᴜꜱ ᴄᴏᴍᴍᴀɴᴅꜱ`,
                            highlight_label: '',
                            rows: [
                                {
                                    title: 'ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴍᴀɴᴅꜱ ⬇️',
                                    description: 'ᴛᴏ ɢᴇᴛ ʙᴏᴛ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴍᴅꜱ 🧭',
                                    id: `${config.PREFIX}downloadmenu`,
                                },
                                {
                                    title: 'ᴍᴀɪɴ ᴄᴏᴍᴍᴀɴᴅꜱ 🌐',
                                    description: 'ᴛᴏ ɢᴇᴛ ʙᴏᴛ ᴍᴀɪɴ ᴄᴍᴅꜱ 🧭',
                                    id: `${config.PREFIX}mainmenu`,
                                },
                                {
                                    title: 'ꜱᴇᴀʀᴄʜ ᴄᴏᴍᴍᴀɴᴅꜱ 🔎',
                                    description: 'ᴛᴏ ɢᴇᴛ ʙᴏᴛ ꜱᴇᴀʀᴄʜ ᴄᴍᴅꜱ 🧭',
                                    id: `${config.PREFIX}searchmenu`,
                                },
                                {
                                    title: 'ᴏᴡɴᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ 👨‍🔧',
                                    description: 'ᴛᴏ ɢᴇᴛ ᴏᴡɴᴇʀ ᴏɴʟʏ ᴄᴍᴅꜱ 🧭',
                                    id: `${config.PREFIX}ownermenu`,
                                },
                                {
                                    title: 'ᴄᴏɴᴠᴇʀᴛ ᴄᴏᴍᴍᴀɴᴅꜱ ⚙️',
                                    description: 'ᴛᴏ ɢᴇᴛ ʙᴏᴛ ᴄᴏɴᴠᴇʀᴛ ᴛᴏᴏʟꜱ 🧭',
                                    id: `${config.PREFIX}convertmenu`,
                                },
                                {
                                    title: 'ꜱᴇᴛᴛɪɴɢꜱ ᴄᴏᴍᴍᴀɴᴅꜱ ⚙️',
                                    description: 'ᴛᴏ ᴄʜᴀɴɢᴇ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ 🧭',
                                    id: `${config.PREFIX}settings`,
                                },
                            ],
                        },
                    ],
                }),
            },
        }
    ];

    await socket.sendMessage(sender, {
        buttons: templateButtons,
        headerType: 1,
        viewOnce: true,
        image: { url: "https://d.uguu.se/JcHekQtn.jpg" },
        caption: `${captionText}`,
    }, { quoted: msg });

    break;
}  
                case 'downloadmenu': {
                    await socket.sendMessage(sender, {
                        react: {
                            text: '⬇️',
                            key: msg.key
                        }
                    });

                    let teksnya = `*_Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｄᴏᴡɴʟᴏᴀᴅ Ｍᴇɴᴜ ☃️_*

*╭──◉ Ｄᴏᴡɴʟᴏᴀᴅ Ｃᴏᴍᴍᴀɴᴅꜱ ◉*
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜱᴏɴɢ
*│* \`ℹ️ :\` ᴅᴏᴡɴʟᴏᴀᴅ ᴀᴜᴅɪᴏ ꜰʀᴏᴍ ʏᴛ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴠɪᴅᴇᴏ
*│* \`ℹ️ :\` ᴅᴏᴡɴʟᴏᴀᴅ ᴠɪᴅᴇᴏ ꜰʀᴏᴍ ʏᴛ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴄꜱᴏɴɢ
*│* \`ℹ️ :\` ꜱᴇɴᴅ ꜱᴏɴɢ ᴛᴏ ᴡʜᴀᴛꜱᴀᴘᴘ ᴄʜᴀɴɴᴇʟ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜱᴏɴɢ2
*│* \`ℹ️ :\` ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ꜱᴏɴɢꜱ ꜰʀᴏᴍ ʏᴛ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴠɪᴅᴇᴏ2
*│* \`ℹ️ :\` ᴅᴏᴡɴʟᴏᴀᴅ ᴠɪᴅᴇᴏ ꜰʀᴏᴍ ʏᴛ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴛɪᴋᴛᴏᴋ
*│* \`ℹ️ :\` ᴅᴏᴡɴʟᴏᴀᴅ ᴠɪᴅᴇᴏ ꜰʀᴏᴍ ᴛʜᴇ ᴛɪᴋᴛᴏᴋ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ɢᴅʀɪᴠᴇ 
*│* \`ℹ️ :\` ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ɢᴏᴏɢʟᴇ ᴅʀɪᴠᴇ ꜰɪʟᴇ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴍᴇᴅɪᴀꜰɪʀᴇ
*│* \`ℹ️ :\` ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴇᴅɪᴀꜰɪʀᴇ ꜰɪʟᴇ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .xɴxx 
*│* \`ℹ️ :\` ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ xxx ᴠɪᴅᴇᴏ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜰᴀᴄᴇʙᴏᴏᴋ
*│* \`ℹ️ :\` ᴅᴏᴡɴʟᴏᴀᴅ ᴠɪᴅᴇᴏ ꜰʀᴏᴍ ꜰᴀᴄᴇʙᴏᴏᴋ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴀᴘᴋ
*│* \`ℹ️ :\` ᴛᴏ ᴅᴏᴡɴʟᴏᴀᴅ ᴀᴘᴘ ꜰʀᴏᴍ ᴘʟᴀʏ ꜱᴛᴏʀᴇ
*╰────────────◉*

*🌐 Ｎᴇxᴜꜱ Ｍᴅ Ｗᴇʙꜱɪᴛᴇ*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    let imageUrl = "https://d.uguu.se/JcHekQtn.jpg";

                    const buttons = [{
                            buttonId: `${config.PREFIX}menu`,
                            buttonText: {
                                displayText: '© Ｂᴀᴄᴋ Ｔᴏ Ｍᴇɴᴜ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}alive`,
                            buttonText: {
                                displayText: '© Ａʟɪᴠᴇ Ｃᴍᴅ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: teksnya,
                        image: {
                            url: 'https://d.uguu.se/JcHekQtn.jpg'
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });
                    break;
                }
                case 'mainmenu': {
                    await socket.sendMessage(sender, {
                        react: {
                            text: '🌐',
                            key: msg.key
                        }
                    });
                    let teksnya = `*_Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｍᴀɪɴ Ｍᴇɴᴜ ☃️_*

*╭──◉ Ｍᴀɪɴ Ｃᴏᴍᴍᴀɴᴅꜱ ◉*
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ʙᴏᴛ ʙᴏᴛ ᴀʟʟ ᴄᴏᴍᴍᴀɴᴅ ʟɪꜱᴛꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴅᴏᴡɴʟᴏᴀᴅᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ʙᴏᴛ ᴀʟʟ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴏᴍᴍᴀɴᴅꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴍᴀɪɴᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ʙᴏᴛ ᴀʟʟ ᴍᴀɪɴ ᴄᴏᴍᴍᴀɴᴅꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜱᴇᴀʀᴄʜᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ᴀʟʟ ꜱᴇᴀʀᴄʜ ᴄᴏᴍᴍᴀɴᴅꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴄᴏɴᴠᴇʀᴛᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ᴀʟʟ ᴛʜᴇ ᴄᴏɴᴠᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴏᴡɴᴇʀᴍᴇɴᴜ
*│* \`ℹ️ :\` ꜱʜᴏᴡ ᴛʜᴇ ᴀʟʟ ᴏᴡɴᴇʀ ᴄᴏᴍᴍᴀɴᴅꜱ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴘɪɴɢ 
*│* \`ℹ️ :\` ᴄʜᴇᴄᴋ ʙᴏᴛ ꜱᴘᴇᴇᴅ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜰᴀɴᴄʏ 
*│* \`ℹ️ :\` ᴛᴏ ɢᴇᴛ ꜰᴀɴᴄʏ ᴛᴇxᴛ ꜱʏʏʟᴇ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ɢᴇᴛᴅᴘ 
*│* \`ℹ️ :\` ᴛᴏ ᴜꜱᴇʀ ᴅᴏᴡɴʟᴏᴀᴅ ᴅᴘ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴀʟɪᴠᴇ
*│* \`ℹ️ :\` ᴄʜᴇᴄᴋ ʙᴏᴛ ᴏɴʟɪɴᴇ ᴏʀ ᴀᴄᴛɪᴠᴇ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜱʏꜱᴛᴇᴍ 
*│* \`ℹ️ :\` ᴄʜᴇᴄᴋ ʙᴏᴛ ꜱʏꜱᴛᴇᴍ ɪɴꜰᴏ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ꜱᴇᴛᴛɪɴɢꜱ 
*│* \`ℹ️ :\` ᴄʜᴇᴄᴋ ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ ᴀɴᴅ ᴄʜᴀɴɢᴇ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴏᴡɴᴇʀ 
*│* \`ℹ️ :\` ᴛᴏ ɢᴇᴛ ᴏᴡɴᴇʀ ɴᴜᴍʙᴇʀ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴘᴀɪʀ
*│* \`ℹ️ :\` ᴛᴏ ᴄᴏɴɴᴇᴄᴛ ᴛʜɪꜱ ʙᴏᴛ
*╰────────────◉*

*🌐 Ｎᴇxᴜꜱ Ｍᴅ Ｗᴇʙꜱɪᴛᴇ*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    let imageUrl = "https://d.uguu.se/JcHekQtn.jpg";

                    const buttons = [{
                            buttonId: `${config.PREFIX}menu`,
                            buttonText: {
                                displayText: '© Ｂᴀᴄᴋ Ｔᴏ Ｍᴇɴᴜ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}alive`,
                            buttonText: {
                                displayText: '© Ａʟɪᴠᴇ Ｃᴍᴅ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: teksnya,
                        image: {
                            url: "https://d.uguu.se/JcHekQtn.jpg"
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });
                    break;
                }
                case 'ownermenu': {
                    await socket.sendMessage(sender, {
                        react: {
                            text: '👨‍🔧',
                            key: msg.key
                        }
                    });

                    let teksnya = `*_Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｏᴡɴᴇʀ Ｍᴇɴᴜ ☃️_*

*╭──◉ Ｏᴡɴᴇʀ Ｃᴏᴍᴍᴀɴᴅꜱ ◉*
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ʙᴏᴏᴍ 
*│* \`ℹ️ :\` ꜱᴇɴᴅ ʙᴏᴏᴍ ᴍᴇꜱꜱᴀɢᴇꜱ
*╰────────────◉*

*🌐 Ｎᴇxᴜꜱ Ｍᴅ Ｗᴇʙꜱɪᴛᴇ*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    let imageUrl = "https://d.uguu.se/JcHekQtn.jpg";

                    const buttons = [{
                            buttonId: `${config.PREFIX}menu`,
                            buttonText: {
                                displayText: '© Ｂᴀᴄᴋ Ｔᴏ Ｍᴇɴᴜ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}alive`,
                            buttonText: {
                                displayText: '© Ａʟɪᴠᴇ Ｃᴍᴅ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: teksnya,
                        image: {
                            url: 'https://d.uguu.se/JcHekQtn.jpg'
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });
                    break;
                }
                case 'convertmenu': {
                    await socket.sendMessage(sender, {
                        react: {
                            text: '📍',
                            key: msg.key
                        }
                    });

                    let teksnya = `*_Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｃᴏɴᴠᴇʀᴛ Ｍᴇɴᴜ ☃️_*

*╭──◉ Ｃᴏɴᴠᴇʀᴛ Ｃᴏᴍᴍᴀɴᴅꜱ ◉*
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴜʀʟ
*│* \`ℹ️ :\` ɪᴍᴀɢᴇ ᴛᴏ ᴜʀʟ ᴄᴏɴᴠᴇʀᴛ
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴛᴏᴜʀʟ
*│* \`ℹ️ :\` ꜰɪxᴇᴅ ᴜʀʟ ᴄᴏᴍᴍᴀɴᴅ
*╰─────────────◉*

*🌐 Ｎᴇxᴜꜱ Ｍᴅ Ｗᴇʙꜱɪᴛᴇ*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    let imageUrl = "https://d.uguu.se/JcHekQtn.jpg";

                    const buttons = [{
                            buttonId: `${config.PREFIX}menu`,
                            buttonText: {
                                displayText: '© Ｂᴀᴄᴋ Ｔᴏ Ｍᴇɴᴜ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}alive`,
                            buttonText: {
                                displayText: '© Ａʟɪᴠᴇ Ｃᴍᴅ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: teksnya,
                        image: {
                            url: 'https://d.uguu.se/JcHekQtn.jpg'
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });
                    break;
                }
                
                case 'searchmenu': {
                    await socket.sendMessage(sender, {
                        react: {
                            text: '🔎',
                            key: msg.key
                        }
                    });

                    let teksnya = `*_Ｎᴇxᴜꜱ Ｍᴅ Ｂᴏᴛ Ｓᴇᴀʀᴄʜ Ｍᴇɴᴜ ☃️_*

*╭──◉ Ｓᴇᴀʀᴄʜ Ｃᴏᴍᴍᴀɴᴅꜱ ◉*
*│*
*│* \`📖 ᴄᴏᴍᴍᴀɴᴅ :\` .ᴛꜱ 
*│* \`ℹ️ :\` ᴛᴏ ꜱᴇᴀʀᴄʜ ᴛɪᴋᴛᴏᴋ
*╰────────────◉*

*🌐 Ｎᴇxᴜꜱ Ｍᴅ Ｗᴇʙꜱɪᴛᴇ*
> ᴄᴏᴍɪɴɢ ꜱᴏᴏɴ

> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;

                    let imageUrl = "https://d.uguu.se/JcHekQtn.jpg";

                    const buttons = [{
                            buttonId: `${config.PREFIX}menu`,
                            buttonText: {
                                displayText: '© ʙᴀᴄᴋ ᴛᴏ ᴍᴇɴᴜ'
                            },
                            type: 1
                        },
                        {
                            buttonId: `${config.PREFIX}alive`,
                            buttonText: {
                                displayText: '© ᴀʟɪᴠᴇ ᴄᴍᴅ'
                            },
                            type: 1
                        }
                    ];

                    await socket.sendMessage(sender, {
                        buttons,
                        headerType: 1,
                        viewOnce: true,
                        caption: teksnya,
                        image: {
                            url: 'https://d.uguu.se/JcHekQtn.jpg'
                        },
                        contextInfo: {
                            mentionedJid: [sender],
                            forwardingScore: 999,
                            isForwarded: true,
                            forwardedNewsletterMessageInfo: {
                                newsletterJid: '120363425542933159@newsletter',
                                newsletterName: 'Ｎᴇxᴜꜱ Ｍᴅ 📌',
                                serverMessageId: 143
                            }
                        }
                    }, {
                        quoted: dtzminibot
                    });
                    break;
                }

// ------------------------------------------------- TIKTOK SEARCH --------------------------------------------------
                case 'ts': {
                    const axios = require('axios');

                    const q = msg.message?.conversation ||
                        msg.message?.extendedTextMessage?.text ||
                        msg.message?.imageMessage?.caption ||
                        msg.message?.videoMessage?.caption || '';

                    const query = q.replace(/^[.\/!]ts\s*/i, '').trim();

                    if (!query) {
                        return await socket.sendMessage(sender, {
                            text: 'ᴘʟᴇᴀꜱᴇ ɢɪᴠᴇ ᴍᴇ ᴀ ꜱᴇᴀʀᴄʜ Qᴜᴀʀʏ 🔍'
                        }, {
                            quoted: msg
                        });
                    }

                    async function tiktokSearch(query) {
                        try {
                            const searchParams = new URLSearchParams({
                                keywords: query,
                                count: '10',
                                cursor: '0',
                                HD: '1'
                            });

                            const response = await axios.post("https://tikwm.com/api/feed/search", searchParams, {
                                headers: {
                                    'Content-Type': "application/x-www-form-urlencoded; charset=UTF-8",
                                    'Cookie': "current_language=en",
                                    'User-Agent': "Mozilla/5.0"
                                }
                            });

                            const videos = response.data?.data?.videos;
                            if (!videos || videos.length === 0) {
                                return {
                                    status: false,
                                    result: "No videos found."
                                };
                            }

                            return {
                                status: true,
                                result: videos.map(video => ({
                                    description: video.title || "No description",
                                    videoUrl: video.play || ""
                                }))
                            };
                        } catch (err) {
                            return {
                                status: false,
                                result: err.message
                            };
                        }
                    }

                    function shuffleArray(array) {
                        for (let i = array.length - 1; i > 0; i--) {
                            const j = Math.floor(Math.random() * (i + 1));
                            [array[i], array[j]] = [array[j], array[i]];
                        }
                    }

                    try {
                        const searchResults = await tiktokSearch(query);
                        if (!searchResults.status) throw new Error(searchResults.result);

                        const results = searchResults.result;
                        shuffleArray(results);

                        const selected = results.slice(0, 6);

                        const cards = await Promise.all(selected.map(async (vid) => {
                            const videoBuffer = await axios.get(vid.videoUrl, {
                                responseType: "arraybuffer"
                            });

                            const media = await prepareWAMessageMedia({
                                video: videoBuffer.data
                            }, {
                                upload: socket.waUploadToServer
                            });

                            return {
                                body: proto.Message.InteractiveMessage.Body.fromObject({
                                    text: ''
                                }),
                                footer: proto.Message.InteractiveMessage.Footer.fromObject({
                                    text: "© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ"
                                }),
                                header: proto.Message.InteractiveMessage.Header.fromObject({
                                    title: vid.description,
                                    hasMediaAttachment: true,
                                    videoMessage: media.videoMessage
                                }),
                                nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({
                                    buttons: []
                                })
                            };
                        }));

                        const msgContent = generateWAMessageFromContent(sender, {
                            viewOnceMessage: {
                                message: {
                                    messageContextInfo: {
                                        deviceListMetadata: {},
                                        deviceListMetadataVersion: 2
                                    },
                                    interactiveMessage: proto.Message.InteractiveMessage.fromObject({
                                        body: {
                                            text: `_*Ｗᴇʟᴄᴏᴍᴇ Ｔᴏ Ｎᴇxᴜꜱ Ｍᴅ Ｍɪɴɪ ☃️"*_\n\n🔎 *ᴛɪᴋᴛᴏᴋ ꜱᴇᴀʀᴄʜ:* ${query}`
                                        },
                                        footer: {
                                            text: "> *© ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*"
                                        },
                                        header: {
                                            hasMediaAttachment: false
                                        },
                                        carouselMessage: {
                                            cards
                                        }
                                    })
                                }
                            }
                        }, {
                            quoted: dtzminibot
                        });

                        await socket.relayMessage(sender, msgContent.message, {
                            messageId: msgContent.key.id
                        });
                    } catch (err) {
                        await socket.sendMessage(sender, {
                            text: `❌ Eʀʀᴏʀ: ${err.message}`
                        }, {
                            quoted: dtzminibot
                        });
                    }
                    break;
                }
                
// ----------------------------- MAIN SETTING CASE -----------------------------
case 'settings': {
  await socket.sendMessage(sender, { react: { text: '⚙️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      return await socket.sendMessage(sender, { text: '❌ Pᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ꜱᴇᴛᴛɪɴɢꜱ.' }, { quoted: msg });
    }
    
    const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
    const botName = "Ｑᴜᴇᴇɴ Ｅʟꜱᴀ Ｍɪɴɪ";
    const prefix = currentConfig.PREFIX || config.PREFIX;
    const logo = "https://d.uguu.se/JcHekQtn.jpg'";
    const stat = (val) => val === 'true' || val === true || val === 'on' ? 'Oɴ' : 'Oꜰꜰ';
    const text = `*ɴᴇxᴜꜱ ᴍᴅ ꜱᴇᴛᴛɪɴɢꜱ 🛡️*
    
🛡️ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ – A ɴᴇᴡ ᴇʀᴀ ᴏꜰ ᴡʜᴀᴛꜱᴀᴘᴘ ʙᴏᴛ ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ ⚡

> ᴏᴡɴᴇʀ ʙʏ ɴᴇxᴜꜱ ᴍɪɴɪ ᴄᴏᴅᴇʀꜱ ᴛᴍ 💥

➟

*╭────────────────┈⊷*
*┋*\`▫️ᴡᴏʀᴋ ᴛɪᴘᴇ Cᴜʀʀᴇɴᴛ:\` ${currentConfig.WORK_TYPE || 'ᴘᴜʙʟɪᴄ'}
*┋*
*┋•*${prefix}ᴡᴛʏᴘᴇ ᴘᴜʙʟɪᴄ
*┋•*${prefix}ᴡᴛʏᴘᴇ ᴘʀɪᴠᴀᴛᴇ
*┋•*${prefix}ᴡᴛʏᴘᴇ ɢʀᴏᴜᴘꜱ
*┋•*${prefix}ᴡᴛʏᴘᴇ ɪɴʙᴏx
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ\` (${stat(currentConfig.AUTO_TYPING)})
*┋*
*┋•*${prefix}ᴀᴜᴛᴏᴛʏᴘɪɴɢ ᴏɴ
*┋•*${prefix}ᴀᴜᴛᴏᴛʏᴘɪɴɢ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ\` (${stat(currentConfig.AUTO_RECORDING)})
*┋*
*┋•*${prefix}ᴀᴜᴛᴏʀᴇᴄᴏʀᴅɪɴɢ ᴏɴ
*┋•*${prefix}ᴀᴜᴛᴏʀᴇᴄᴏʀᴅɪɴɢ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀʟᴡᴀʏꜱ ᴏɴʟɪɴᴇ\` (${currentConfig.PRESENCE || 'ᴏꜰꜰʟɪɴᴇ'})
*┋*
*┋•*${prefix}ʙᴏᴛᴘʀᴇꜱᴇɴᴄᴇ ᴏɴʟɪɴᴇ
*┋•*${prefix}ʙᴏᴛᴘʀᴇꜱᴇɴᴄᴇ ᴏꜰꜰʟɪɴᴇ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ\` (${stat(currentConfig.AUTO_VIEW_STATUS)})
*┋*
*┋•*${prefix}ʀꜱᴛᴀᴛᴜꜱ ᴏɴ
*┋•*${prefix}ʀꜱᴛᴀᴛᴜꜱ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ʀᴇᴀᴄᴛ\` (${stat(currentConfig.AUTO_LIKE_STATUS)})
*┋*
*┋•*${prefix}ᴀʀᴍ ᴏɴ
*┋•*${prefix}ᴀʀᴍ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟꜱ\` (${stat(currentConfig.ANTI_CALL)})
*┋*
*┋•*${prefix}ᴄʀᴇᴊᴇᴄᴛ ᴏɴ
*┋•*${prefix}ᴄʀᴇᴊᴇᴄᴛ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
*╭────────────────┈⊷*
*┋*\`▫️ᴀᴜᴛᴏ ᴍᴀꜱꜱᴀɢᴇꜱ ʀᴇᴀᴅ\` (${currentConfig.AUTO_READ_MESSAGE || 'ᴏꜰꜰ'})
*┋*
*┋•*${prefix}ᴍʀᴇᴀᴅ ᴀʟʟ
*┋•*${prefix}ᴍʀᴇᴀᴅ ᴄᴍᴅ
*┋•*${prefix}ᴍʀᴇᴀᴅ ᴏꜰꜰ
*┋*
*╰────────────────┈⊷*
> 📣 *ʀᴇᴘʟʏ ᴡɪᴛʜ ᴛʜᴇ ᴄᴏᴍᴍᴀɴᴅ ɴᴇᴇᴅᴇᴅ*

> *©  ɴᴇxᴜꜱ ᴍᴅ ꜱᴇᴛᴛɪɴɢ ᴘᴀɴɴᴇʟ*
> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ ɴᴇxᴜꜱ ᴍᴅ ᴍɪɴɪ*`;
    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: text,
      buttons: [{ buttonId: `${prefix}menu`, buttonText: { displayText: "📂 Bᴀᴄᴋ Tᴏ Mᴇɴᴜ" }, type: 1 }],
      headerType: 4
    }, { quoted: msg });
  } catch (e) {
    console.error('Setting command error:', e);
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ʟᴏᴀᴅɪɴɢ ꜱᴇᴛᴛɪɴɢꜱ!*" }, { quoted: msg });
  }
  break;
}

// ----------------------------- WORK TYPE -----------------------------
case 'wtype': {
  await socket.sendMessage(sender, { react: { text: '🛠️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴡᴏʀᴋ ᴛʏᴘᴇ' }, { quoted: msg });
    }
    
    let q = args[0];
    const settings = {
      groups: "ɢʀᴏᴜᴘꜱ",
      inbox: "ɪɴʙᴏx", 
      private: "ᴘʀɪᴠᴀᴛᴇ",
      public: "ᴘᴜʙʟɪᴄ"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.WORK_TYPE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nNＮᴇxᴜꜱ Ｍᴅ;;;;\nFN:Ｎᴇxᴜꜱ Ｍᴅ\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *ʏᴏᴜʀ ᴡᴏʀᴋ ᴛʏᴘᴇ ᴜᴘᴅᴀᴛᴇᴅ ᴛᴏ: ${settings[q]}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Iɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴘᴜʙʟɪᴄ / ɢʀᴏᴜᴘꜱ / ɪɴʙᴏx / ᴘʀɪᴠᴀᴛᴇ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Wtype command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ᴡᴏʀᴋ ᴛʏᴘᴇ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- BOT PRESENCE -----------------------------
case 'botpresence': {
  await socket.sendMessage(sender, { react: { text: '👨‍🔧', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = {
      online: "ᴀᴠᴀɪʟᴀʙʟᴇ",
      offline: "ᴜɴᴀᴠᴀʟɪᴀʙʟᴇ"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.PRESENCE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      await socket.sendPresenceUpdate(settings[q]);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *ʏᴏᴜʀ ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ ᴜᴘᴅᴀᴛᴇᴅ ᴛᴏ: ${q}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Iɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴏɴʟɪɴᴇ / ᴏꜰꜰʟɪɴᴇ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Botpresence command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- AUTO TYPING -----------------------------
case 'autotyping': {
  await socket.sendMessage(sender, { react: { text: '⌨️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "ᴛʀᴜᴇ", off: "ꜰᴀʟꜱᴇ" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_TYPING = settings[q];
      
      if (q === 'on') {
        userConfig.AUTO_RECORDING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Aᴜᴛᴏ ᴛʏᴘɪɴɢ ${q === 'on' ? 'Eɴᴀʙʟᴇᴅ' : 'Dɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "\`❌ ᴏᴘᴛɪᴏɴꜱ :\` ᴏɴ / ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autotyping error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- AUTO READ STATUS -----------------------------
case 'rstatus': {
  await socket.sendMessage(sender, { react: { text: '👁️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "ᴛʀᴜᴇ", off: "ꜰᴀʟꜱᴇ" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_VIEW_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Yᴏᴜʀ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ${q === 'on' ? 'Eɴᴀʙʟᴇᴅ' : 'Dɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Iɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴꜱ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴏɴ / ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Rstatus command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ꜱᴇᴛᴛɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- AUTO CALL REJECT -----------------------------
case 'creject': {
  await socket.sendMessage(sender, { react: { text: '📞', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴀᴜᴛᴏ ᴄᴀʟʟ ʀᴇᴊᴇᴄᴛ.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "ᴏɴ", off: "ᴏꜰꜰ" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.ANTI_CALL = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Yᴏᴜʀ ᴀᴜᴛᴏ ᴄᴀʟʟ ʀᴇᴊᴇᴄᴛ ${q === 'on' ? 'Eɴᴀʙʟᴇᴅ' : 'Dɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Iɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴꜱ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴏɴ / ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Creject command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ᴄᴀʟʟ ʀᴇᴊᴇᴄᴛ ꜱᴇᴛᴛɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- AUTO STATUS LIKE -----------------------------
case 'arm': {
  await socket.sendMessage(sender, { react: { text: '🖤', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ʀᴇᴀᴄᴛ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴛᴛɪɴɢ' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "ᴛʀᴜᴇ", off: "ꜰᴀʟꜱᴇ" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_LIKE_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Yᴏᴜʀ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ʀᴇᴀᴄᴛ ${q === 'on' ? 'Eɴᴀʙʟᴇᴅ' : 'Dɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *ɪɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴꜱ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴏɴ / ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Arm command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ꜱᴛᴀᴛᴜꜱ ʀᴇᴀᴄᴛ ꜱᴇᴛᴛɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- MESSAGE READ -----------------------------
case 'mread': {
  await socket.sendMessage(sender, { react: { text: '📖', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴍᴇꜱꜱᴀɢᴇ ʀᴇᴀᴅ' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { all: "ᴀʟʟ", cmd: "ᴄᴍᴅ", off: "ᴏꜰꜰ" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_READ_MESSAGE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      let statusText = "";
      switch (q) {
        case "all":
          statusText = "ʀᴇᴀᴅ ᴀʟʟ ᴍᴇꜱꜱᴀɢᴇꜱ";
          break;
        case "cmd":
          statusText = "ʀᴇᴀᴅ ᴏɴʟʏ ᴄᴏᴍᴍᴀɴᴅ ᴍᴇꜱꜱᴀɢᴇꜱ"; 
          break;
        case "off":
          statusText = "ᴅᴏɴᴛ ʀᴇᴀᴅ ᴀɴʏ ᴍᴇꜱꜱᴀɢᴇꜱ";
          break;
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Yᴏᴜʀ ᴀᴜᴛᴏ ᴍᴇꜱꜱᴀɢᴇ ʀᴇᴀᴅ: ${statusText}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Iɴᴠᴀʟɪᴅ ᴏᴘᴛɪᴏɴꜱ!*\n\n\`ᴀᴠᴀɪʟᴀʙʟᴇ ᴏᴘᴛɪᴏɴꜱ :\` ᴀʟʟ / ᴄᴍᴅ / ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Mread command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ᴍᴇꜱꜱᴀɢᴇ ʀᴇᴀᴅ ꜱᴇᴛᴛɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- AUTO RECORDING -----------------------------
case 'autorecording': {
  await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ' }, { quoted: shonux });
    }
    
    let q = args[0];
    
    if (q === 'on' || q === 'off') {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_RECORDING = (q === 'on') ? "ᴛʀᴜᴇ" : "ꜰᴀʟꜱᴇ";
      
      if (q === 'on') {
        userConfig.AUTO_TYPING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      if (q === 'off') {
        await socket.sendPresenceUpdate('available', sender);
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Aᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ ${q === 'on' ? 'Eɴᴀʙʟᴇᴅ' : 'Dɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "\`❌ Iɴᴠᴀʟɪᴅ! ᴜꜱᴇ :\` .ᴀᴜᴛᴏʀᴇᴄᴏʀᴅɪɴɢ ᴏɴ/ᴏꜰꜰ" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autorecording error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- CHANGE PREFIX -----------------------------
case 'prefix': {
  await socket.sendMessage(sender, { react: { text: '🔣', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢᴇ ᴘʀᴇꜰɪx' }, { quoted: shonux });
    }
    
    let newPrefix = args[0];
    if (!newPrefix || newPrefix.length > 2) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: "❌ *ɪɴᴠᴀʟɪᴅ ᴘʀᴇꜰɪx*." }, { quoted: shonux });
    }
    
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    userConfig.PREFIX = newPrefix;
    await setUserConfigInMongo(sanitized, userConfig);
    
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `✅ *ʏᴏᴜʀ ᴘʀᴇꜰɪx ᴜᴘᴅᴀᴛᴇᴅ ᴛᴏ: ${newPrefix}*` }, { quoted: shonux });
  } catch (e) {
    console.error('Prefix command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Eʀʀᴏʀ ᴜᴘᴅᴀᴛɪɴɢ ʏᴏᴜʀ ᴘʀᴇꜰɪx!*" }, { quoted: shonux });
  }
  break;
}

// ----------------------------- SHOW YOUR CONFIG -----------------------------
case 'showconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  try {
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let txt = `*Sᴇꜱꜱɪᴏɴ ᴄᴏɴꜰɪɢ ꜰᴏʀ ${sanitized}:*\n`;
    txt += `• Bᴏᴛ ɴᴀᴍᴇ : ${botName}\n`;
    txt += `• Lᴏɢᴏ : ${cfg.logo || config.RCD_IMAGE_PATH}\n\n`;
    txt += `> *® ᴘᴏᴡᴇʀᴅᴇᴅ ʙʏ Qᴜᴇᴇɴ ᴍɪɴᴜᴜ ᴍɪɴɪ*`;
    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('showconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Fᴀɪʟᴇᴅ ᴛᴏ ʟᴏᴀᴅ ᴄᴏɴꜰɪɢ.' }, { quoted: shonux });
  }
  break;
}

// ----------------------------- RESET YOUR CONFIG -----------------------------
case 'resetconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʜᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ʀᴇꜱᴇᴛ ᴄᴏɴꜰɪɢꜱ.' }, { quoted: shonux });
    break;
  }

  try {
    await setUserConfigInMongo(sanitized, {});

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '✅ Sᴇꜱꜱɪᴏɴ ᴄᴏɴꜰɪɢ ʀᴇꜱᴇᴛ ᴛᴏ ᴅᴇꜰᴀᴜʟᴛꜱ.' }, { quoted: shonux });
  } catch (e) {
    console.error('resetconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '❌ Fᴀɪʟᴇᴅ ᴛᴏ ʀᴇꜱᴇᴛ ᴄᴏɴꜰɪɢ.' }, { quoted: shonux });
  }
  break;
}

// ----------------------------- SET BOT LOGO -----------------------------
case 'setlogo': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏɴʟʏ ᴛʏᴇ ꜱᴇꜱꜱɪᴏɴ ᴏᴡɴᴇʀ ᴏʀ ʙᴏᴛ ᴏᴡɴᴇʀ ᴄᴀɴ ᴄʜᴀɴɢʀ ᴛʜɪꜱ ꜱᴇꜱꜱɪᴏɴ ʟᴏɢᴏ.' }, { quoted: shonux });
    break;
  }

  const ctxInfo = (msg.message.extendedTextMessage || {}).contextInfo || {};
  const quotedMsg = ctxInfo.quotedMessage;
  const media = await downloadQuotedMedia(quotedMsg).catch(()=>null);
  let logoSetTo = null;

  try {
    if (media && media.buffer) {
      const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
      fs.ensureDirSync(sessionPath);
      const mimeExt = (media.mime && media.mime.split('/').pop()) || 'jpg';
      const logoPath = path.join(sessionPath, `logo.${mimeExt}`);
      fs.writeFileSync(logoPath, media.buffer);
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = logoPath;
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = logoPath;
    } else if (args && args[0] && (args[0].startsWith('http') || args[0].startsWith('https'))) {
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = args[0];
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = args[0];
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: '❗ Uꜱᴀɢᴇ : Rᴇᴘʟʏ ᴛᴏ ᴀɴ ɪᴍᴀɢᴇ ᴡɪᴛʜ .ꜱᴇᴛʟᴏɢᴏ' }, { quoted: shonux });
      break;
    }

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Lᴏɢᴏ ꜱᴇᴛ ꜰᴏʀ ᴛʜɪꜱ ꜱᴇꜱꜱɪᴏɴ : ${logoSetTo}` }, { quoted: shonux });
  } catch (e) {
    console.error('setlogo error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `\`❌ Fᴀɪʟᴇᴅ ᴛᴏ ꜱᴇᴛ ʟᴏɢᴏ:\` ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

// ----------------------------- SET BOT NAME -----------------------------
case 'setbotname': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ ᴘᴇʀᴍɪꜱꜱɪᴏɴ ᴅᴇɴɪᴇᴅ. ᴏᴡɴᴇʀ ᴏɴʟʏ ᴛʜɪꜱ ᴄᴏᴍᴍᴀɴᴅ.' }, { quoted: shonux });
    break;
  }

  const name = args.join(' ').trim();
  if (!name) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❗ Pʀᴏᴠɪᴅᴇ ᴀ ʙᴏᴛ ɴᴀᴍᴇ ᴡɪᴛʜ .ꜱᴇᴛʙᴏᴛɴᴀᴍᴇ' }, { quoted: shonux });
  }

  try {
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    cfg.botName = name;
    await setUserConfigInMongo(sanitized, cfg);

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Bᴏᴛ ᴅɪꜱᴘʟᴀʏ ɴᴀᴍᴇ ꜱᴇᴛ ꜰᴏʀ ᴛʜɪꜱ ꜱᴇꜱꜱɪᴏɴ: ${name}` }, { quoted: shonux });
  } catch (e) {
    console.error('setbotname error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `\`❌ Fᴀɪʟᴇᴅ ᴛᴏ ꜱᴇᴛ ʙᴏᴛ ɴᴀᴍᴇ :\` ${e.message || e}` }, { quoted: shonux });
  }
  break;
}
        default:
          break;
      }
    } catch (err) {
      console.error('Command handler error:', err);
      try { await socket.sendMessage(sender, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('❌ ERROR', 'An error occurred while processing your command. Please try again.', BOT_NAME_FANCY) }); } catch(e){}
    }

  });
}

// ---------------- message handlers ----------------

function setupMessageHandlers(socket) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;
    if (config.AUTO_RECORDING === 'true') {
      try { await socket.sendPresenceUpdate('recording', msg.key.remoteJid); } catch (e) {}
    }
  });
}

// ---------------- cleanup helper ----------------

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
    activeSockets.delete(sanitized); socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch(e){}
    try { await removeNumberFromMongo(sanitized); } catch(e){}
    try {
      const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
      const caption = formatMessage('👑 OWNER NOTICE — SESSION REMOVED', `Number: ${sanitized}\nSession removed due to logout.\n\nActive sessions now: ${activeSockets.size}`, BOT_NAME_FANCY);
      if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
    } catch(e){}
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ---------------- auto-restart ----------------

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
                         || lastDisconnect?.error?.statusCode
                         || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
                          || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
                          || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
                          || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch(e){ console.error(e); }
      } else {
        console.log(`Connection closed for ${number} (not logout). Attempt reconnect...`);
        try { await delay(10000); activeSockets.delete(number.replace(/[^0-9]/g,'')); socketCreationTime.delete(number.replace(/[^0-9]/g,'')); const mockRes = { headersSent:false, send:() => {}, status: () => mockRes }; await EmpirePair(number, mockRes); } catch(e){ console.error('Reconnect attempt failed', e); }
      }

    }

  });
}

// ---------------- EmpirePair (pairing, temp dir, persist to Mongo) ----------------

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  await initMongo().catch(()=>{});
  // Prefill from Mongo if available
  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

 try {
    const socket = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      printQRInTerminal: false,
      logger,
      browser: ["Ubuntu", "Chrome", "20.0.04"]
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    setupStatusHandlers(socket);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    // Save creds to Mongo when updated
    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        const fileContent = await fs.readFile(path.join(sessionPath, 'creds.json'), 'utf8');
        const credsObj = JSON.parse(fileContent);
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
      } catch (err) { console.error('Failed saving creds on creds.update:', err); }
    });


    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(()=>({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid);
             await socket.newsletterFollow('120363425542933159@newsletter');
                } catch(e){}
            }
          } catch(e){}




          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error}`;

          // Load per-session config (botName, logo)
          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `*\`PLEASE WAIT...!\`*`,
            useBotName
          );

          // send initial message
          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            console.warn('Failed to send initial connect message (image). Falling back to text.', e?.message || e);
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch(e){}
          }

          await delay(4000);

          const updatedCaption = formatMessage(useBotName,
            `✅ සාර්ථකව සම්බන්ධ වී, දැන් ක්‍රියාත්මකයි!\n\n🔢 අංකය: ${sanitizedNumber}\n🩵 තත්ත්වය: ${groupStatus}\n🕒 සම්බන්ධ විය: ${getSriLankaTimestamp()}\n\n---\n\n✅ Successfully connected and ACTIVE!\n\n🔢 Number: ${sanitizedNumber}\n🩵 Status: ${groupStatus}\n🕒 Connected at: ${getSriLankaTimestamp()}`,
            useBotName
          );

          try {
            if (sentMsg && sentMsg.key) {
              try {
                await socket.sendMessage(userJid, { delete: sentMsg.key });
              } catch (delErr) {
                console.warn('Could not delete original connect message (not fatal):', delErr?.message || delErr);
              }
            }

            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption });
            }
          } catch (e) {
            console.error('Failed during connect-message edit sequence:', e);
          }

          // send admin + owner notifications as before, with session overrides
          await sendAdminConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await sendOwnerConnectMessage(socket, sanitizedNumber, groupResult, userConfig);
          await addNumberToMongo(sanitizedNumber);

        } catch (e) { 
          console.error('Connection open error:', e); 
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'NIKKA-MINI-main'}`); } catch(e) { console.error('pm2 restart failed', e); }
        }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
      }

    });


    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }

}


// ---------------- endpoints (admin/newsletter management + others) ----------------

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// admin endpoints

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// existing endpoints (connect, reconnect, active, etc.)

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});


router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});


router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: '🇱🇰SAMURAI XMD', activesession: activeSockets.size });
});


router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});


router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});


router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});


router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});


router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target number are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    const aboutStatus = statusData.status || 'No status available';
    const setAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
    res.status(200).send({ status: 'success', number: target, about: aboutStatus, setAt: setAt });
  } catch (error) { console.error(`Failed to fetch status for ${target}:`, error); res.status(500).send({ status: 'error', message: `Failed to fetch About status for ${target}.` }); }
});


// ---------------- Dashboard endpoints & static ----------------

const dashboardStaticDir = path.join(__dirname, 'dashboard_static');
if (!fs.existsSync(dashboardStaticDir)) fs.ensureDirSync(dashboardStaticDir);
router.use('/dashboard/static', express.static(dashboardStaticDir));
router.get('/dashboard', async (req, res) => {
  res.sendFile(path.join(dashboardStaticDir, 'index.html'));
});


// API: sessions & active & delete

router.get('/api/sessions', async (req, res) => {
  try {
    await initMongo();
    const docs = await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, sessions: docs });
  } catch (err) {
    console.error('API /api/sessions error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(()=>{}); } catch(e){}
      try { running.ws?.close(); } catch(e){}
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized}`); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch(e){}
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    console.error('API /api/session/delete error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});
router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


// ---------------- cleanup + process events ----------------

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch(e){}
  });
});


process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  try { exec(`pm2.restart ${process.env.PM2_NAME || 'NIKKA-MINI-main'}`); } catch(e) { console.error('Failed to restart pm2:', e); }
});


// initialize mongo & auto-reconnect attempt

initMongo().catch(err => console.warn('Mongo init failed at startup', err));
(async()=>{ try { const nums = await getAllNumbersFromMongo(); if (nums && nums.length) { for (const n of nums) { if (!activeSockets.has(n)) { const mockRes = { headersSent:false, send:()=>{}, status:()=>mockRes }; await EmpirePair(n, mockRes); await delay(500); } } } } catch(e){} })();

module.exports = router;
