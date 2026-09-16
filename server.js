const MESSAGE_TIMEOUT_MS=6000;
const SESSION_MAX_AGE_SECONDS=60*60*24*30;
const http=require('http');
const fs=require('fs');
const path=require('path');
const crypto=require('crypto');
const {promisify}=require('util');
const WebSocket=require('ws');
const {Pool}=require('pg');
const scrypt=promisify(crypto.scrypt);
if(!process.env.DATABASE_URL){
    console.error('DATABASE_URL is required');
    process.exit(1);
}
const pool=new Pool({
    connectionString:process.env.DATABASE_URL,
    ssl:{rejectUnauthorized:false}
});
const server=http.createServer(async(req,res)=>{
    let filePath=path.join(__dirname,'public','index.html');
    if(req.method==='GET'&&req.url==='/'){
        await handleWebsiteVisit(req,res);
        return;
    }
    if(req.method==='POST'&&req.url==='/api/register'){
        await handleRegister(req,res);
        return;
    }
    if(req.method==='POST'&&req.url==='/api/login'){
        await handleLogin(req,res);
        return;
    }
    if(req.method==='POST'&&req.url==='/api/logout'){
        await handleLogout(req,res);
        return;
    }
    if(req.method==='GET'&&req.url==='/api/me'){
        await handleMe(req,res);
        return;
    }
    if(req.method==='GET'&&req.url==='/api/siteInfo'){
        await handleSiteInfo(req,res);
        return;
    }
    res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});
    res.end('Not Found');
});
const wss=new WebSocket.Server({server});
const rooms=new Map();
const activeNames=new Map();
const matchmakingQueue=new Set();
let nextPlayerId=1;
function send(ws,data){
    if(ws&&ws.readyState===WebSocket.OPEN){
        ws.send(JSON.stringify(data));
    }
}
function showMessage(ws,message){
    send(ws,{
        type:'message',
        message:message,
        timeout:MESSAGE_TIMEOUT_MS
    });
}
function broadcastOnlineCount(){
    let count=0;
    wss.clients.forEach(ws=>{
        if(ws.readyState===WebSocket.OPEN&&ws.playerName){
            count++;
        }
    });
    wss.clients.forEach(ws=>{
        if(ws.readyState===WebSocket.OPEN){
            send(ws,{
                type:'onlineCount',
                count:count
            });
        }
    });
}
function normalizeName(name){
    return name.toLocaleLowerCase();
}
function createRoomCode(){
    let code;
    do{
        code=String(Math.floor(100000+Math.random()*900000));
    }while(rooms.has(code));
    return code;
}
function getPlayer(room,role){
    if(role==='host'){
        return room.host;
    }
    if(role==='guest'){
        return room.guest;
    }
    return null;
}
function sendRoomState(room,ws){
    if(!room.host){
        return;
    }
    if(ws===room.host.ws){
        send(ws,{
            type:'roomUpdate',
            yourRole:'host',
            roomCode:room.code,
            range:room.range,
            phase:room.phase,
            host:{
                name:room.host.name,
                ready:room.host.ready
            },
            guest:room.guest?{
                name:room.guest.name,
                ready:room.guest.ready
            }:null
        });
        return;
    }
    if(room.guest&&ws===room.guest.ws){
        send(ws,{
            type:'roomUpdate',
            yourRole:'guest',
            roomCode:room.code,
            range:room.range,
            phase:room.phase,
            host:{
                name:room.host.name,
                ready:room.host.ready
            },
            guest:{
                name:room.guest.name,
                ready:room.guest.ready
            }
        });
    }
}
function broadcastRoom(room){
    if(room.host){
        sendRoomState(room,room.host.ws);
    }
    if(room.guest){
        sendRoomState(room,room.guest.ws);
    }
}
function clearStartTimer(room){
    if(room.startTimer){
        clearTimeout(room.startTimer);
        room.startTimer=null;
    }
}
function clearRoundEndTimer(room){
    if(room.roundEndTimer){
        clearTimeout(room.roundEndTimer);
        room.roundEndTimer=null;
    }
}
function truthResponse(room){
    if(room.currentGuess<room.answer){
        return 'higher';
    }
    if(room.currentGuess>room.answer){
        return 'lower';
    }
    return 'correct';
}
function responseText(response,guess){
    if(response==='higher'){
        return `答案比${guess}大`;
    }
    if(response==='lower'){
        return `答案比${guess}小`;
    }
    return '猜中了';
}
function removeFromMatchmaking(ws){
    matchmakingQueue.delete(ws);
}
function sendGameReset(room,player,message){
    if(!player){
        return;
    }
    send(player.ws,{
        type:'gameReset',
        message:message,
        timeout:MESSAGE_TIMEOUT_MS,
        roomCode:room.code,
        yourRole:'host',
        range:room.range,
        phase:room.phase,
        host:{
            name:room.host.name,
            ready:room.host.ready
        },
        guest:null
    });
}
function resetGameRoom(room,leavingWs){
    clearStartTimer(room);
    clearRoundEndTimer(room);
    const wasHost=room.host&&room.host.ws===leavingWs;
    const wasGuest=room.guest&&room.guest.ws===leavingWs;
    if(!wasHost&&!wasGuest){
        return;
    }
    const otherPlayer=wasHost?room.guest:room.host;
    if(!otherPlayer){
        rooms.delete(room.code);
        send(leavingWs,{
            type:'backToMenu'
        });
        leavingWs.roomCode=null;
        leavingWs.role=null;
        return;
    }
    room.host={
        ws:otherPlayer.ws,
        name:otherPlayer.name,
        ready:false
    };
    room.guest=null;
    otherPlayer.ws.roomCode=room.code;
    otherPlayer.ws.role='host';
    room.phase=room.range===null?'range':'waiting';
    room.round=0;
    room.attackerRole=null;
    room.defenderRole=null;
    room.answer=null;
    room.step=0;
    room.lieUsed=false;
    room.currentGuess=null;
    room.lastResponse=null;
    room.roundHistory=[];
    room.roundSteps={
        host:null,
        guest:null
    };
    sendGameReset(
        room,
        otherPlayer,
        room.range===null
            ?'對方已退出，現在由你擔任房主，請選擇遊戲範圍'
            :'對方已退出，現在由你擔任房主'
    );
    send(leavingWs,{
        type:'backToMenu'
    });
    leavingWs.roomCode=null;
    leavingWs.role=null;
    console.log(`Game reset after player left: ${room.code}`);
}
function transferConnection(oldWs,newWs){
    removeFromMatchmaking(oldWs);
    if(oldWs.roomCode){
        const room=rooms.get(oldWs.roomCode);
        if(room){
            if(room.host&&room.host.ws===oldWs){
                room.host.ws=newWs;
            }
            if(room.guest&&room.guest.ws===oldWs){
                room.guest.ws=newWs;
            }
            newWs.roomCode=oldWs.roomCode;
            newWs.role=oldWs.role;
            if(room.phase==='range'||
               room.phase==='waiting'||
               room.phase==='starting'){
                sendRoomState(room,newWs);
            }else if(room.phase==='game'||
                    room.phase==='answer'||
                    room.phase==='guess'||
                    room.phase==='lieChoice'){
                sendGameState(room,newWs);
            }else if(room.phase==='roundEnd'){
                sendRoundEndState(room,newWs);
            }else if(room.phase==='finished'){
                sendFinishedState(room,newWs);
            }
        }
    }
    oldWs.ignoreClose=true;
    oldWs.roomCode=null;
    oldWs.role=null;
    try{
        oldWs.close();
    }catch(e){
    }
}
function createMatchmakingRoom(player1,player2){
    removeFromMatchmaking(player1);
    removeFromMatchmaking(player2);
    let host;
    let guest;
    if(Math.random()<0.5){
        host=player1;
        guest=player2;
    }else{
        host=player2;
        guest=player1;
    }
    const code=createRoomCode();
    const room={
        code:code,
        host:{
            ws:host,
            name:host.playerName,
            ready:false
        },
        guest:{
            ws:guest,
            name:guest.playerName,
            ready:false
        },
        range:null,
        phase:'range',
        startTimer:null,
        roundEndTimer:null,
        round:0,
        attackerRole:null,
        defenderRole:null,
        answer:null,
        step:0,
        lieUsed:false,
        currentGuess:null,
        lastResponse:null,
        roundHistory:[],
        roundSteps:{
            host:null,
            guest:null
        },
        statsRecorded:false
    };
    rooms.set(code,room);
    host.roomCode=code;
    host.role='host';
    guest.roomCode=code;
    guest.role='guest';
    console.log(`Random match: ${host.playerName} (host) vs ${guest.playerName} (guest), room ${code}`);
    send(host,{
        type:'matchFound',
        message:'已找到對手'
    });
    send(guest,{
        type:'matchFound',
        message:'已找到對手'
    });
    broadcastRoom(room);
}
function tryMatchmaking(){
    for(const ws of matchmakingQueue){
        if(ws.readyState!==WebSocket.OPEN||!ws.playerName||ws.roomCode){
            matchmakingQueue.delete(ws);
        }
    }
    const players=[...matchmakingQueue];
    for(let i=players.length-1;i>0;i--){
        const j=Math.floor(Math.random()*(i+1));
        [players[i],players[j]]=[players[j],players[i]];
    }
    for(let i=0;i+1<players.length;i+=2){
        createMatchmakingRoom(players[i],players[i+1]);
    }
}
setInterval(()=>{
    tryMatchmaking();
},10000);
function startRound(room,round){
    room.round=round;
    if(round===1){
        room.attackerRole='host';
        room.defenderRole='guest';
    }else{
        room.attackerRole='guest';
        room.defenderRole='host';
    }
    room.answer=null;
    room.step=0;
    room.lieUsed=false;
    room.currentGuess=null;
    room.lastResponse=null;
    room.roundHistory=[];
    room.phase='answer';
    broadcastGameState(room);
}
function startGame(room){
    startRound(room,1);
}
function sendGameState(room,ws){
    if(!room.host||!room.guest){
        return;
    }
    const role=ws===room.host.ws?'host':'guest';
    const attacker=getPlayer(room,room.attackerRole);
    const defender=getPlayer(room,room.defenderRole);
    if(!attacker||!defender){
        return;
    }
    const data={
        type:'gameState',
        roomCode:room.code,
        range:room.range,
        round:room.round,
        phase:room.phase,
        step:room.step,
        currentGuess:role===room.defenderRole?room.currentGuess:null,
        lastResponse:role===room.attackerRole?room.lastResponse:null,
        roundHistory:room.roundHistory,
        yourRole:role,
        attacker:{
            name:attacker.name
        },
        defender:{
            name:defender.name
        },
        attackerIsYou:role===room.attackerRole,
        defenderIsYou:role===room.defenderRole,
        lieUsed:room.lieUsed
    };
    if(room.phase==='lieChoice'&&role===room.defenderRole){
        const truth=truthResponse(room);
        const lie=truth==='higher'?'lower':'higher';
        data.truthText=responseText(truth,room.currentGuess);
        data.lieText=responseText(lie,room.currentGuess);
        data.canLie=!room.lieUsed;
    }
    send(ws,data);
}
function broadcastGameState(room){
    if(room.host){
        sendGameState(room,room.host.ws);
    }
    if(room.guest){
        sendGameState(room,room.guest.ws);
    }
}
function sendRoundEnd(room){
    const attacker=getPlayer(room,room.attackerRole);
    const data={
        type:'roundEnd',
        round:room.round,
        attacker:attacker.name,
        steps:room.step,
        answer:room.answer,
        roundHistory:room.roundHistory,
        timeout:MESSAGE_TIMEOUT_MS
    };
    if(room.round===1){
        room.roundSteps[room.attackerRole]=room.step;
        send(room.host.ws,data);
        if(room.guest){
            send(room.guest.ws,data);
        }
        room.phase='roundEnd';
        clearRoundEndTimer(room);
        room.roundEndTimer=setTimeout(()=>{
            const currentRoom=rooms.get(room.code);
            if(!currentRoom){
                return;
            }
            currentRoom.roundEndTimer=null;
            if(currentRoom.phase!=='roundEnd'){
                return;
            }
            if(!currentRoom.host||
               !currentRoom.guest||
               currentRoom.host.ws.readyState!==WebSocket.OPEN||
               currentRoom.guest.ws.readyState!==WebSocket.OPEN){
                return;
            }
            startRound(currentRoom,2);
        },MESSAGE_TIMEOUT_MS);
        return;
    }
    room.roundSteps[room.attackerRole]=room.step;
    room.phase='finished';
    recordMatchResult(room).then(()=>{
        sendFinalResult(room,data);
    }).catch(error=>{
        console.error(`Failed to record match result for room ${room.code}:`,error);
        sendFinalResult(room,data);
    });
}
async function recordMatchResult(room){
    if(room.statsRecorded){
        return;
    }
    const hostSteps=room.roundSteps.host;
    const guestSteps=room.roundSteps.guest;
    if(!Number.isInteger(hostSteps)||!Number.isInteger(guestSteps)){
        throw new Error('Invalid round steps');
    }
    const host=getPlayer(room,'host');
    const guest=getPlayer(room,'guest');
    if(!host||!guest){
        throw new Error('Players are missing');
    }
    let result;
    if(hostSteps<guestSteps){
        result='hostWin';
    }else if(hostSteps>guestSteps){
        result='guestWin';
    }else{
        result='draw';
    }
    const client=await pool.connect();
    try{
        await client.query('BEGIN');
        if(host.ws.userId!==null){
            await client.query(
                `INSERT INTO user_stats(user_id,wins,losses,draws)
                 VALUES($1,0,0,0)
                 ON CONFLICT(user_id) DO NOTHING`,
                [host.ws.userId]
            );
        }
        if(guest.ws.userId!==null){
            await client.query(
                `INSERT INTO user_stats(user_id,wins,losses,draws)
                 VALUES($1,0,0,0)
                 ON CONFLICT(user_id) DO NOTHING`,
                [guest.ws.userId]
            );
        }
        if(result==='hostWin'){
            if(host.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET wins=wins+1 WHERE user_id=$1',
                    [host.ws.userId]
                );
            }
            if(guest.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET losses=losses+1 WHERE user_id=$1',
                    [guest.ws.userId]
                );
            }
        }else if(result==='guestWin'){
            if(host.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET losses=losses+1 WHERE user_id=$1',
                    [host.ws.userId]
                );
            }
            if(guest.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET wins=wins+1 WHERE user_id=$1',
                    [guest.ws.userId]
                );
            }
        }else{
            if(host.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET draws=draws+1 WHERE user_id=$1',
                    [host.ws.userId]
                );
            }
            if(guest.ws.userId!==null){
                await client.query(
                    'UPDATE user_stats SET draws=draws+1 WHERE user_id=$1',
                    [guest.ws.userId]
                );
            }
        }
        await client.query(
            'UPDATE site_stats SET completed_games=completed_games+1 WHERE id=1'
        );
        await client.query('COMMIT');
        room.statsRecorded=true;
        console.log(`Match result recorded: ${host.name} vs ${guest.name} (${result})`);
    }catch(error){
        try{
            await client.query('ROLLBACK');
        }catch(e){
        }
        throw error;
    }finally{
        client.release();
    }
}
function sendFinalResult(room,roundEndData){
    const hostSteps=room.roundSteps.host;
    const guestSteps=room.roundSteps.guest;
    let resultText;
    if(hostSteps<guestSteps){
        resultText=`${room.host.name} 以 ${hostSteps} 步擊敗 ${room.guest.name} 的 ${guestSteps} 步`;
    }else if(hostSteps>guestSteps){
        resultText=`${room.guest.name} 以 ${guestSteps} 步擊敗 ${room.host.name} 的 ${hostSteps} 步`;
    }else{
        resultText=`${room.host.name} 的 ${hostSteps} 步與 ${room.guest.name} 的 ${guestSteps} 步打成平手`;
    }
    const data={
        type:'finalResult',
        hostName:room.host.name,
        guestName:room.guest.name,
        hostSteps:hostSteps,
        guestSteps:guestSteps,
        resultText:resultText,
        round:roundEndData.round,
        attacker:roundEndData.attacker,
        roundSteps:roundEndData.steps,
        answer:roundEndData.answer,
        roundHistory:roundEndData.roundHistory
    };
    send(room.host.ws,data);
    send(room.guest.ws,data);
}
function sendRoundEndState(room,ws){
    const attacker=getPlayer(room,room.attackerRole);
    if(!attacker){
        return;
    }
    send(ws,{
        type:'roundEnd',
        round:room.round,
        attacker:attacker.name,
        steps:room.step,
        answer:room.answer,
        roundHistory:room.roundHistory,
        timeout:MESSAGE_TIMEOUT_MS
    });
}
function sendFinishedState(room,ws){
    const hostSteps=room.roundSteps.host;
    const guestSteps=room.roundSteps.guest;
    let resultText;
    if(hostSteps<guestSteps){
        resultText=`${room.host.name} 以 ${hostSteps} 步擊敗 ${room.guest.name} 的 ${guestSteps} 步`;
    }else if(hostSteps>guestSteps){
        resultText=`${room.guest.name} 以 ${guestSteps} 步擊敗 ${room.host.name} 的 ${hostSteps} 步`;
    }else{
        resultText=`${room.host.name} 的 ${hostSteps} 步與 ${room.guest.name} 的 ${guestSteps} 步打成平手`;
    }
    send(ws,{
        type:'finalResult',
        hostName:room.host.name,
        guestName:room.guest.name,
        hostSteps:hostSteps,
        guestSteps:guestSteps,
        resultText:resultText,
        round:2,
        attacker:room.guest.name,
        roundSteps:guestSteps,
        answer:room.answer,
        roundHistory:room.roundHistory
    });
}
function leaveFinishedRoom(room,ws,sendBack=true){
    clearStartTimer(room);
    clearRoundEndTimer(room);
    if(ws===room.host?.ws){
        room.host=null;
    }else if(ws===room.guest?.ws){
        room.guest=null;
    }else{
        return;
    }
    ws.roomCode=null;
    ws.role=null;
    if(sendBack){
        send(ws,{
            type:'backToMenu'
        });
    }
    if(!room.host&&!room.guest){
        rooms.delete(room.code);
        console.log(`Finished room closed: ${room.code}`);
    }else{
        console.log(`${ws.playerName} left finished room ${room.code}`);
    }
}
function removePlayerFromRoom(ws,sendBack=false){
    if(!ws.roomCode){
        return;
    }
    const room=rooms.get(ws.roomCode);
    if(!room){
        ws.roomCode=null;
        ws.role=null;
        return;
    }
    if(room.phase==='finished'){
        leaveFinishedRoom(room,ws,sendBack);
        return;
    }
    if(room.phase==='starting'||
       room.phase==='game'||
       room.phase==='answer'||
       room.phase==='guess'||
       room.phase==='lieChoice'||
       room.phase==='roundEnd'){
        resetGameRoom(room,ws);
        return;
    }
    const wasHost=ws.role==='host';
    clearStartTimer(room);
    clearRoundEndTimer(room);
    if(!room.guest){
        rooms.delete(room.code);
        send(ws,{
            type:'backToMenu'
        });
        ws.roomCode=null;
        ws.role=null;
        console.log(`Room closed: ${room.code}`);
        return;
    }
    if(wasHost){
        const newHost=room.guest;
        room.host={
            ws:newHost.ws,
            name:newHost.name,
            ready:false
        };
        room.guest=null;
        newHost.ws.role='host';
        newHost.ws.roomCode=room.code;
        room.phase=room.range===null?'range':'waiting';
        send(newHost.ws,{
            type:'playerLeft',
            message:'對方已退出，你現在是房主',
            timeout:MESSAGE_TIMEOUT_MS
        });
        send(ws,{
            type:'backToMenu'
        });
        broadcastRoom(room);
        console.log(`${newHost.name} became host of room ${room.code}`);
    }else{
        room.guest=null;
        room.host.ready=false;
        room.phase=room.range===null?'range':'waiting';
        send(room.host.ws,{
            type:'playerLeft',
            message:'對方已退出',
            timeout:MESSAGE_TIMEOUT_MS
        });
        send(ws,{
            type:'backToMenu'
        });
        broadcastRoom(room);
        console.log(`Guest left room ${room.code}`);
    }
    ws.roomCode=null;
    ws.role=null;
}
function getCookies(req){
    const cookies={};
    const header=req.headers.cookie||'';
    header.split(';').forEach(part=>{
        const index=part.indexOf('=');
        if(index<0){
            return;
        }
        const key=part.slice(0,index).trim();
        const value=part.slice(index+1).trim();
        if(key){
            try{
                cookies[key]=decodeURIComponent(value);
            }catch(e){
                cookies[key]=value;
            }
        }
    });
    return cookies;
}
function sessionTokenHash(token){
    return crypto.createHash('sha256').update(token).digest('hex');
}
function setSessionCookie(res,token){
    const cookie=`guessNumberPkSession=${encodeURIComponent(token)}; HttpOnly; SameSite=Lax; Path=/; Max-Age=${SESSION_MAX_AGE_SECONDS}; Secure`;
    res.setHeader('Set-Cookie',cookie);
}
function clearSessionCookie(res){
    res.setHeader('Set-Cookie','guessNumberPkSession=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0; Secure');
}
function setVisitCookie(res,token){
    const cookie=`guessNumberPkVisit=${encodeURIComponent(token)}; HttpOnly; SameSite=Lax; Path=/; Secure`;
    const current=res.getHeader('Set-Cookie');
    if(current){
        if(Array.isArray(current)){
            res.setHeader('Set-Cookie',[...current,cookie]);
        }else{
            res.setHeader('Set-Cookie',[current,cookie]);
        }
    }else{
        res.setHeader('Set-Cookie',cookie);
    }
}
function sendJson(res,status,data){
    res.writeHead(status,{
        'Content-Type':'application/json; charset=utf-8',
        'Cache-Control':'no-store'
    });
    res.end(JSON.stringify(data));
}
function readJson(req){
    return new Promise((resolve,reject)=>{
        let body='';
        req.on('data',chunk=>{
            body+=chunk.toString();
            if(body.length>1024*1024){
                reject(new Error('body too large'));
                req.destroy();
            }
        });
        req.on('end',()=>{
            try{
                resolve(JSON.parse(body||'{}'));
            }catch(e){
                reject(new Error('invalid json'));
            }
        });
        req.on('error',reject);
    });
}
function validUsername(username){
    if(typeof username!=='string'){
        return false;
    }
    const value=username.trim();
    if(value===''){
        return false;
    }
    if(value.length>20){
        return false;
    }
    return true;
}
function validPassword(password){
    return typeof password==='string'&&password.length>=6&&password.length<=200;
}
async function createPasswordHash(password){
    const salt=crypto.randomBytes(16).toString('hex');
    const derivedKey=await scrypt(password,salt,64,{
        N:16384,
        r:8,
        p:1,
        maxmem:32*1024*1024
    });
    return `scrypt$16384$8$1$${salt}$${derivedKey.toString('hex')}`;
}
async function verifyPassword(password,passwordHash){
    const parts=passwordHash.split('$');
    if(parts.length!==6||parts[0]!=='scrypt'){
        return false;
    }
    const N=Number(parts[1]);
    const r=Number(parts[2]);
    const p=Number(parts[3]);
    const salt=parts[4];
    const stored=Buffer.from(parts[5],'hex');
    if(!Number.isInteger(N)||
       !Number.isInteger(r)||
       !Number.isInteger(p)||
       stored.length===0){
        return false;
    }
    try{
        const derived=await scrypt(password,salt,stored.length,{
            N:N,
            r:r,
            p:p,
            maxmem:32*1024*1024
        });
        return derived.length===stored.length&&
            crypto.timingSafeEqual(derived,stored);
    }catch(e){
        return false;
    }
}
async function createSession(userId){
    const token=crypto.randomBytes(32).toString('hex');
    const tokenHash=sessionTokenHash(token);
    await pool.query('DELETE FROM sessions WHERE expires_at<NOW()');
    await pool.query(
        'INSERT INTO sessions(token_hash,user_id,expires_at) VALUES($1,$2,NOW()+INTERVAL \'30 days\')',
        [tokenHash,userId]
    );
    return token;
}
async function getSessionUser(req){
    const token=getCookies(req).guessNumberPkSession;
    if(!token){
        return null;
    }
    const tokenHash=sessionTokenHash(token);
    const result=await pool.query(
        `SELECT u.id,u.username
         FROM sessions s
         JOIN users u ON u.id=s.user_id
         WHERE s.token_hash=$1 AND s.expires_at>NOW()`,
        [tokenHash]
    );
    if(result.rowCount===0){
        return null;
    }
    await pool.query(
        'UPDATE sessions SET expires_at=NOW()+INTERVAL \'30 days\' WHERE token_hash=$1',
        [tokenHash]
    );
    return result.rows[0];
}
async function handleRegister(req,res){
    let data;
    try{
        data=await readJson(req);
    }catch(e){
        sendJson(res,400,{ok:false,message:'資料格式錯誤'});
        return;
    }
    const username=String(data.username||'').trim();
    const password=typeof data.password==='string'?data.password:'';
    const confirmPassword=typeof data.confirmPassword==='string'?data.confirmPassword:'';
    if(!validUsername(username)){
        sendJson(res,400,{
            ok:false,
            message:username===''?'帳號不能是空白':'帳號不能超過20個字'
        });
        return;
    }
    if(!validPassword(password)){
        sendJson(res,400,{ok:false,message:'密碼長度需為6到200個字元'});
        return;
    }
    if(password!==confirmPassword){
        sendJson(res,400,{ok:false,message:'兩次輸入的密碼不一致'});
        return;
    }
    const usernameKey=normalizeName(username);
    try{
        const existing=await pool.query(
            'SELECT id FROM users WHERE username_key=$1',
            [usernameKey]
        );
        if(existing.rowCount>0){
            sendJson(res,409,{ok:false,message:'帳號已有人使用'});
            return;
        }
        const passwordHash=await createPasswordHash(password);
        const result=await pool.query(
            'INSERT INTO users(username,username_key,password_hash) VALUES($1,$2,$3) RETURNING id,username',
            [username,usernameKey,passwordHash]
        );
        const token=await createSession(result.rows[0].id);
        setSessionCookie(res,token);
        sendJson(res,200,{
            ok:true,
            username:result.rows[0].username
        });
    }catch(e){
        if(e.code==='23505'){
            sendJson(res,409,{ok:false,message:'帳號已有人使用'});
            return;
        }
        console.error('Register error:',e);
        sendJson(res,500,{ok:false,message:'伺服器發生錯誤'});
    }
}
async function handleLogin(req,res){
    let data;
    try{
        data=await readJson(req);
    }catch(e){
        sendJson(res,400,{ok:false,message:'資料格式錯誤'});
        return;
    }
    const username=String(data.username||'').trim();
    const password=typeof data.password==='string'?data.password:'';
    if(username===''||password===''){
        sendJson(res,400,{ok:false,message:'請輸入帳號與密碼'});
        return;
    }
    try{
        const result=await pool.query(
            'SELECT id,username,password_hash FROM users WHERE username_key=$1',
            [normalizeName(username)]
        );
        if(result.rowCount===0){
            sendJson(res,401,{ok:false,message:'帳號或密碼錯誤'});
            return;
        }
        const user=result.rows[0];
        const matched=await verifyPassword(password,user.password_hash);
        if(!matched){
            sendJson(res,401,{ok:false,message:'帳號或密碼錯誤'});
            return;
        }
        const token=await createSession(user.id);
        setSessionCookie(res,token);
        sendJson(res,200,{
            ok:true,
            username:user.username
        });
    }catch(e){
        console.error('Login error:',e);
        sendJson(res,500,{ok:false,message:'伺服器發生錯誤'});
    }
}
async function handleLogout(req,res){
    const token=getCookies(req).guessNumberPkSession;
    if(token){
        try{
            await pool.query(
                'DELETE FROM sessions WHERE token_hash=$1',
                [sessionTokenHash(token)]
            );
        }catch(e){
            console.error('Logout error:',e);
        }
    }
    clearSessionCookie(res);
    sendJson(res,200,{ok:true});
}
async function handleMe(req,res){
    try{
        const user=await getSessionUser(req);
        if(!user){
            sendJson(res,200,{
                ok:true,
                authenticated:false
            });
            return;
        }
        sendJson(res,200,{
            ok:true,
            authenticated:true,
            username:user.username
        });
    }catch(e){
        console.error('Session check error:',e);
        sendJson(res,500,{ok:false,message:'伺服器發生錯誤'});
    }
}
async function handleWebsiteVisit(req,res){
    const cookies=getCookies(req);
    let isNewVisit=false;
    if(!cookies.guessNumberPkVisit){
        isNewVisit=true;
    }
    if(isNewVisit){
        const visitToken=crypto.randomBytes(32).toString('hex');
        setVisitCookie(res,visitToken);
        try{
            await pool.query(
                'UPDATE site_stats SET visit_count=visit_count+1 WHERE id=1'
            );
        }catch(e){
            console.error('Visit count error:',e);
        }
    }
    const filePath=path.join(__dirname,'public','index.html');
    fs.readFile(filePath,(err,data)=>{
        if(err){
            res.writeHead(500,{'Content-Type':'text/plain; charset=utf-8'});
            res.end('Server error');
            return;
        }
        const cookiesHeader=res.getHeader('Set-Cookie');
        res.writeHead(200,{
            'Content-Type':'text/html; charset=utf-8',
            ...(cookiesHeader?{'Set-Cookie':cookiesHeader}:{})
        });
        res.end(data);
    });
}
async function handleSiteInfo(req,res){
    try{
        const result=await pool.query(`
            SELECT
                s.visit_count::int AS visit_count,
                s.completed_games::int AS completed_games,
                COUNT(u.id)::int AS account_count
            FROM site_stats s
            LEFT JOIN users u ON TRUE
            WHERE s.id=1
            GROUP BY s.visit_count,s.completed_games
        `);
        if(result.rowCount===0){
            sendJson(res,500,{ok:false,message:'網站資料不存在'});
            return;
        }
        const row=result.rows[0];
        sendJson(res,200,{
            ok:true,
            visitCount:row.visit_count,
            accountCount:row.account_count,
            gameCount:row.completed_games
        });
    }catch(e){
        console.error('Site info error:',e);
        sendJson(res,500,{ok:false,message:'伺服器發生錯誤'});
    }
}
async function authenticateSocket(req){
    const token=getCookies(req).guessNumberPkSession;
    if(!token){
        return null;
    }
    const tokenHash=sessionTokenHash(token);
    const result=await pool.query(
        `SELECT u.id,u.username
         FROM sessions s
         JOIN users u ON u.id=s.user_id
         WHERE s.token_hash=$1 AND s.expires_at>NOW()`,
        [tokenHash]
    );
    if(result.rowCount===0){
        return null;
    }
    await pool.query(
        'UPDATE sessions SET expires_at=NOW()+INTERVAL \'30 days\' WHERE token_hash=$1',
        [tokenHash]
    );
    return {
        id:result.rows[0].id,
        username:result.rows[0].username,
        tokenHash:tokenHash
    };
}
function getGuestRequest(req){
    let url;
    try{
        url=new URL(
            req.url||'/',
            `http://${req.headers.host||'localhost'}`
        );
    }catch(e){
        return {
            requested:false,
            name:null
        };
    }
    if(url.searchParams.get('guest')!=='1'){
        return {
            requested:false,
            name:null
        };
    }
    const name=(url.searchParams.get('playerName')||'').trim();
    return {
        requested:true,
        name:name
    };
}
wss.on('connection',async(ws,req)=>{
    ws.playerId=nextPlayerId++;
    ws.playerName=null;
    ws.userId=null;
    ws.sessionTokenHash=null;
    ws.roomCode=null;
    ws.role=null;
    ws.isGuest=false;
    ws.isAlive=true;
    ws.ignoreClose=false;
    try{
        const guestRequest=getGuestRequest(req);
        if(guestRequest.requested){
            if(!validUsername(guestRequest.name)){
                send(ws,{
                    type:'authError',
                    message:'訪客名稱必須為1到20個字元'
                });
                ws.close(4001,'Invalid guest name');
                return;
            }
            ws.isGuest=true;
            ws.userId=null;
            ws.playerName=guestRequest.name;
        }else{
            const user=await authenticateSocket(req);
            if(!user){
                send(ws,{
                    type:'authRequired'
                });
                ws.close(4001,'Authentication required');
                return;
            }
            ws.userId=user.id;
            ws.playerName=user.username;
            ws.sessionTokenHash=user.tokenHash;
        }
        const nameKey=normalizeName(ws.playerName);
        const existing=activeNames.get(nameKey);
        if(existing&&existing.ws!==ws){
            if(existing.userId!==ws.userId||
               existing.isGuest!==ws.isGuest){
                send(ws,{
                    type:'authError',
                    message:'這個玩家名稱目前已有人在線'
                });
                ws.close(4001,'Player name conflict');
                return;
            }
            transferConnection(existing.ws,ws);
            activeNames.delete(nameKey);
        }
        activeNames.set(nameKey,{
            ws:ws,
            userId:ws.userId,
            isGuest:ws.isGuest
        });
        console.log(`Player connected: ${ws.playerId}, name: ${ws.playerName}, guest: ${ws.isGuest}`);
        send(ws,{
            type:'authenticated',
            name:ws.playerName,
            guest:ws.isGuest
        });
        broadcastOnlineCount();
    }catch(e){
        console.error('WebSocket authentication error:',e);
        try{
            ws.close(1011,'Authentication error');
        }catch(error){
        }
        return;
    }
    ws.on('pong',()=>{
        ws.isAlive=true;
    });
    ws.on('message',async(message)=>{
        let data;
        try{
            data=JSON.parse(message);
        }catch(e){
            return;
        }
        if(typeof data.type!=='string'){
            return;
        }
        if(data.type==='findMatch'){
            if(ws.roomCode){
                return;
            }
            if(matchmakingQueue.has(ws)){
                return;
            }
            matchmakingQueue.add(ws);
            console.log(`${ws.playerName} entered matchmaking`);
            send(ws,{
                type:'matchmakingWaiting'
            });
            return;
        }
        if(data.type==='cancelMatch'){
            if(!matchmakingQueue.has(ws)){
                return;
            }
            removeFromMatchmaking(ws);
            console.log(`${ws.playerName} left matchmaking`);
            send(ws,{
                type:'backToMenu'
            });
            return;
        }
        if(data.type==='createRoom'){
            if(ws.roomCode){
                return;
            }
            removeFromMatchmaking(ws);
            const code=createRoomCode();
            const room={
                code:code,
                host:{
                    ws:ws,
                    name:ws.playerName,
                    ready:false
                },
                guest:null,
                range:null,
                phase:'range',
                startTimer:null,
                roundEndTimer:null,
                round:0,
                attackerRole:null,
                defenderRole:null,
                answer:null,
                step:0,
                lieUsed:false,
                currentGuess:null,
                lastResponse:null,
                roundHistory:[],
                roundSteps:{
                    host:null,
                    guest:null
                },
                statsRecorded:false
            };
            rooms.set(code,room);
            ws.roomCode=code;
            ws.role='host';
            console.log(`Room created: ${code} by ${ws.playerName}`);
            broadcastRoom(room);
            return;
        }
        if(data.type==='joinRoom'){
            if(ws.roomCode){
                return;
            }
            removeFromMatchmaking(ws);
            const code=String(data.code||'').trim();
            const room=rooms.get(code);
            if(!room){
                showMessage(ws,'房間不存在');
                return;
            }
            if(room.phase==='starting'||
               room.phase==='game'||
               room.phase==='answer'||
               room.phase==='guess'||
               room.phase==='lieChoice'||
               room.phase==='roundEnd'||
               room.phase==='finished'){
                showMessage(ws,'房間已關閉');
                return;
            }
            if(room.guest){
                showMessage(ws,'已有兩人加入');
                return;
            }
            room.guest={
                ws:ws,
                name:ws.playerName,
                ready:false
            };
            ws.roomCode=code;
            ws.role='guest';
            if(room.range===null){
                room.phase='range';
            }else{
                room.phase='waiting';
            }
            console.log(`${ws.playerName} joined room ${code}`);
            broadcastRoom(room);
            return;
        }
        if(data.type==='setRange'){
            if(!ws.roomCode||ws.role!=='host'){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||room.phase!=='range'){
                return;
            }
            const range=Number(data.range);
            if(range!==50&&range!==100&&range!==200){
                showMessage(ws,'範圍不合法');
                return;
            }
            room.range=range;
            room.host.ready=false;
            if(room.guest){
                room.guest.ready=false;
            }
            room.phase='waiting';
            broadcastRoom(room);
            return;
        }
        if(data.type==='ready'){
            if(!ws.roomCode){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||
               room.phase!=='waiting'||
               !room.guest||
               room.range===null){
                return;
            }
            const player=getPlayer(room,ws.role);
            if(!player){
                return;
            }
            player.ready=true;
            broadcastRoom(room);
            if(room.host.ready&&room.guest.ready){
                room.phase='starting';
                clearStartTimer(room);
                room.startTimer=setTimeout(()=>{
                    const currentRoom=rooms.get(room.code);
                    if(!currentRoom){
                        return;
                    }
                    currentRoom.startTimer=null;
                    if(currentRoom.phase!=='starting'){
                        return;
                    }
                    if(!currentRoom.host.ready||
                       !currentRoom.guest||
                       !currentRoom.guest.ready||
                       currentRoom.range===null){
                        currentRoom.phase='waiting';
                        broadcastRoom(currentRoom);
                        return;
                    }
                    currentRoom.phase='game';
                    startGame(currentRoom);
                },1000);
            }
            return;
        }
        if(data.type==='cancelReady'){
            if(!ws.roomCode){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||room.phase!=='waiting'){
                return;
            }
            const player=getPlayer(room,ws.role);
            if(!player){
                return;
            }
            player.ready=false;
            broadcastRoom(room);
            return;
        }
        if(data.type==='leaveRoom'){
            if(!ws.roomCode){
                if(matchmakingQueue.has(ws)){
                    removeFromMatchmaking(ws);
                    send(ws,{
                        type:'backToMenu'
                    });
                    return;
                }
                send(ws,{
                    type:'backToMenu'
                });
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room){
                ws.roomCode=null;
                ws.role=null;
                send(ws,{
                    type:'backToMenu'
                });
                return;
            }
            if(room.phase==='finished'){
                leaveFinishedRoom(room,ws,true);
                return;
            }
            console.log(`${ws.playerName} left room ${ws.roomCode}`);
            removePlayerFromRoom(ws,true);
            return;
        }
        if(data.type==='setAnswer'){
            if(!ws.roomCode){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||
               room.phase!=='answer'||
               ws.role!==room.defenderRole){
                return;
            }
            const answer=Number(data.answer);
            if(!Number.isInteger(answer)){
                showMessage(ws,'答案必須是整數');
                return;
            }
            if(answer<=0||answer>=room.range){
                showMessage(ws,'答案必須在範圍內，而且不能是邊界');
                return;
            }
            room.answer=answer;
            room.step=1;
            room.currentGuess=null;
            room.lastResponse=null;
            room.lieUsed=false;
            room.phase='guess';
            broadcastGameState(room);
            return;
        }
        if(data.type==='guess'){
            if(!ws.roomCode){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||
               room.phase!=='guess'||
               ws.role!==room.attackerRole){
                return;
            }
            const guess=Number(data.guess);
            if(!Number.isInteger(guess)){
                showMessage(ws,'猜測必須是整數');
                return;
            }
            if(guess<=0||guess>=room.range){
                showMessage(ws,'猜測不能是邊界數字');
                return;
            }
            room.currentGuess=guess;
            room.lastResponse=null;
            if(guess===room.answer){
                room.roundHistory.push({
                    guess:guess,
                    response:'猜中了',
                    lied:false
                });
                room.phase='roundEnd';
                sendRoundEnd(room);
                return;
            }
            room.phase='lieChoice';
            broadcastGameState(room);
            return;
        }
        if(data.type==='lieChoice'){
            if(!ws.roomCode){
                return;
            }
            const room=rooms.get(ws.roomCode);
            if(!room||
               room.phase!=='lieChoice'||
               ws.role!==room.defenderRole){
                return;
            }
            if(data.choice!=='lie'&&data.choice!=='truth'){
                return;
            }
            if(data.choice==='lie'&&room.lieUsed){
                return;
            }
            const truth=truthResponse(room);
            const response=
                data.choice==='truth'
                    ?truth
                    :(truth==='higher'?'lower':'higher');
            const lied=data.choice==='lie';
            if(lied){
                room.lieUsed=true;
            }
            room.lastResponse={
                text:responseText(response,room.currentGuess),
                response:response,
                lied:lied
            };
            room.roundHistory.push({
                guess:room.currentGuess,
                response:room.lastResponse.text,
                lied:lied
            });
            room.step++;
            room.currentGuess=null;
            room.phase='guess';
            broadcastGameState(room);
            return;
        }
    });
    ws.on('close',()=>{
        console.log(`Player disconnected: ${ws.playerName||ws.playerId}`);
        if(ws.ignoreClose){
            return;
        }
        removeFromMatchmaking(ws);
        if(ws.playerName){
            const nameKey=normalizeName(ws.playerName);
            const active=activeNames.get(nameKey);
            if(active&&active.ws===ws){
                activeNames.delete(nameKey);
            }
        }
        removePlayerFromRoom(ws,false);
        broadcastOnlineCount();
        tryMatchmaking();
    });
});
const heartbeat=setInterval(()=>{
    wss.clients.forEach(ws=>{
        if(ws.isAlive===false){
            ws.terminate();
            return;
        }
        ws.isAlive=false;
        ws.ping();
    });
},MESSAGE_TIMEOUT_MS);
wss.on('close',()=>{
    clearInterval(heartbeat);
});
async function initDatabase(){
    await pool.query(`
        CREATE TABLE IF NOT EXISTS users(
            id BIGSERIAL PRIMARY KEY,
            username VARCHAR(20) NOT NULL,
            username_key VARCHAR(20) NOT NULL UNIQUE,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS sessions(
            id BIGSERIAL PRIMARY KEY,
            token_hash CHAR(64) NOT NULL UNIQUE,
            user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            expires_at TIMESTAMPTZ NOT NULL
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS user_stats(
            user_id BIGINT PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
            wins INTEGER NOT NULL DEFAULT 0,
            losses INTEGER NOT NULL DEFAULT 0,
            draws INTEGER NOT NULL DEFAULT 0
        )
    `);
    await pool.query(`
        CREATE TABLE IF NOT EXISTS site_stats(
            id INTEGER PRIMARY KEY,
            visit_count INTEGER NOT NULL DEFAULT 0,
            completed_games INTEGER NOT NULL DEFAULT 0
        )
    `);
    await pool.query(`
        INSERT INTO site_stats(id,visit_count,completed_games)
        VALUES(1,0,0)
        ON CONFLICT(id) DO NOTHING
    `);
    await pool.query(
        'CREATE INDEX IF NOT EXISTS sessions_user_id_idx ON sessions(user_id)'
    );
    await pool.query(
        'CREATE INDEX IF NOT EXISTS sessions_expires_at_idx ON sessions(expires_at)'
    );
    console.log('PostgreSQL ready');
}
const PORT=process.env.PORT||3000;
initDatabase().then(()=>{
    server.listen(PORT,'0.0.0.0',()=>{
        console.log(`Server running on port ${PORT}`);
    });
}).catch(error=>{
    console.error('Database initialization failed:',error);
    process.exit(1);
});
process.on('SIGTERM',async()=>{
    try{
        await pool.end();
    }finally{
        process.exit(0);
    }
});
