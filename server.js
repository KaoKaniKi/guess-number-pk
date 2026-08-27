const MESSAGE_TIMEOUT_MS=6000;
const http=require('http');
const fs=require('fs');
const path=require('path');
const WebSocket=require('ws');
const server=http.createServer((req,res)=>{
    let filePath=path.join(__dirname,'public','index.html');
    if(req.url!=='/'){
        res.writeHead(404,{'Content-Type':'text/plain; charset=utf-8'});
        res.end('Not Found');
        return;
    }
    fs.readFile(filePath,(err,data)=>{
        if(err){
            res.writeHead(500,{'Content-Type':'text/plain; charset=utf-8'});
            res.end('Server error');
            return;
        }
        res.writeHead(200,{'Content-Type':'text/html; charset=utf-8'});
        res.end(data);
    });
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
    leavingWs.roomCode=null;
    leavingWs.role=null;
    sendGameReset(
        room,
        otherPlayer,
        room.range===null
            ?'對方已退出，現在由你擔任房主，請選擇遊戲範圍'
            :'對方已退出，現在由你擔任房主'
    );
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
        }
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
    while(matchmakingQueue.size>=2){
        const players=[...matchmakingQueue];
        const firstIndex=Math.floor(Math.random()*players.length);
        let secondIndex=Math.floor(Math.random()*players.length);
        while(secondIndex===firstIndex){
            secondIndex=Math.floor(Math.random()*players.length);
        }
        const player1=players[firstIndex];
        const player2=players[secondIndex];
        createMatchmakingRoom(player1,player2);
    }
}
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
    sendFinalResult(room,data);
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
        broadcastRoom(room);
        console.log(`Guest left room ${room.code}`);
    }
    ws.roomCode=null;
    ws.role=null;
}
wss.on('connection',(ws)=>{
    ws.playerId=nextPlayerId++;
    ws.playerName=null;
    ws.sessionId=null;
    ws.roomCode=null;
    ws.role=null;
    ws.isAlive=true;
    ws.ignoreClose=false;
    ws.on('pong',()=>{
        ws.isAlive=true;
    });
    console.log(`Player connected: ${ws.playerId}`);
    send(ws,{
        type:'connected'
    });
    ws.on('message',(message)=>{
        let data;
        try{
            data=JSON.parse(message);
        }catch(e){
            return;
        }
        if(typeof data.type!=='string'){
            return;
        }
        if(data.type==='setName'){
            if(ws.playerName!==null){
                return;
            }
            const name=String(data.name||'').trim();
            const sessionId=String(data.sessionId||'');
            if(name===''){
                showMessage(ws,'名稱不能是空白');
                return;
            }
            if(name.length>20){
                showMessage(ws,'名稱不能超過20個字');
                return;
            }
            if(sessionId===''){
                showMessage(ws,'玩家識別失敗，請重新整理頁面');
                return;
            }
            ws.sessionId=sessionId;
            const nameKey=normalizeName(name);
            const existing=activeNames.get(nameKey);
            if(existing){
                if(existing.sessionId!==sessionId){
                    send(ws,{
                        type:'nameTaken',
                        message:'名字有人用了',
                        timeout:MESSAGE_TIMEOUT_MS
                    });
                    return;
                }
                if(existing.ws!==ws){
                    transferConnection(existing.ws,ws);
                }
                activeNames.delete(nameKey);
            }
            ws.playerName=name;
            activeNames.set(nameKey,{
                ws:ws,
                sessionId:sessionId
            });
            console.log(`Player ${ws.playerId} name: ${name}`);
            send(ws,{
                type:'nameSet',
                name:name
            });
            return;
        }
        if(!ws.playerName){
            showMessage(ws,'請先設定名稱');
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
            tryMatchmaking();
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
                }
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
                    response:'猜中了'
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
            if(data.choice==='lie'){
                room.lieUsed=true;
            }
            room.lastResponse={
                text:responseText(response,room.currentGuess),
                response:response
            };
            room.roundHistory.push({
                guess:room.currentGuess,
                response:room.lastResponse.text
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
const PORT=process.env.PORT||3000;
server.listen(PORT,'0.0.0.0',()=>{
    console.log(`Server running on port ${PORT}`);
});
