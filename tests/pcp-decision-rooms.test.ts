import assert from "node:assert/strict";
import { test } from "node:test";
import { decisionRoomsSnapshot, decisionRoomsFilter } from "../src/matrix/pcp-decision-rooms.js";

const event=(type:string,state_key:string,content:unknown)=>({type,state_key,content});
const child=(id:string)=>event("m.space.child",id,{via:["x"]});
function room(name?:string, children?:Array<ReturnType<typeof event>>){
 return {state:{events:[event("m.room.create","",children?{type:"m.space"}:{}),
   ...(name===undefined?[]:[event("m.room.name","",{name})]),...children??[]]}};
}
function wire(rooms:Record<string,ReturnType<typeof room>>, direct:unknown){
 return {next_batch:"opaque",rooms:{join:rooms},account_data:{events:[{type:"m.direct",content:direct}]}};
}

test("trusted account m.direct and joined-space canonical name only",()=>{
  const rooms={"!dm:x":room(),"!two-members:x":room(),
    "!space:x":room(undefined,[child("!ann:x"),child("!not-exact:x"),child("!left:x"),event("m.space.child","!invalid:x",{})]),
    "!ann:x":room(" ANNOUNCEMENTS "),"!not-exact:x":room("Project Announcements"),
    "!invalid:x":room("Announcements"),"!orphan:x":room("Announcements")};
  assert.deepEqual(decisionRoomsSnapshot(wire(rooms,{"@peer:x":["!dm:x","!left:x","!dm:x"]}),Object.keys(rooms),1234),
    {schema:"matrix-decision-rooms-v1",observed_at:1234,direct:["!dm:x"],announcements:["!ann:x"]});
  const next={...rooms,"!ann:x":room("Archive")};
  assert.deepEqual(decisionRoomsSnapshot(wire(next,{"@peer:x":["!two-members:x"]}),Object.keys(next),2345),
    {schema:"matrix-decision-rooms-v1",observed_at:2345,direct:["!two-members:x"],announcements:[]});
});
test("incomplete membership, creation state or account data cannot replace policy",()=>{
  const rooms={"!dm:x":room()};
  assert.equal(decisionRoomsSnapshot(wire(rooms,{"@peer:x":["!dm:x"]}),["!missing:x","!dm:x"]),null);
  assert.equal(decisionRoomsSnapshot(wire(rooms,null),Object.keys(rooms)),null);
  assert.equal(decisionRoomsSnapshot(wire(rooms,{"@peer:x":"!dm:x"}),Object.keys(rooms)),null);
  assert.deepEqual(decisionRoomsFilter.room.timeline,{limit:0,types:[]});
  assert.deepEqual(decisionRoomsFilter.account_data,{types:["m.direct"]});
});

test("authoritative zero direct rooms and empty membership clear previous categories",()=>{
  const rooms={"!space:x":room(undefined,[child("!ann:x")]),"!ann:x":room("Announcements")};
  assert.deepEqual(decisionRoomsSnapshot({next_batch:"opaque",rooms:{join:rooms},account_data:{events:[]}},Object.keys(rooms),1234),
    {schema:"matrix-decision-rooms-v1",observed_at:1234,direct:[],announcements:["!ann:x"]});
  assert.deepEqual(decisionRoomsSnapshot({next_batch:"opaque",rooms:{join:{}}},[],1234),
    {schema:"matrix-decision-rooms-v1",observed_at:1234,direct:[],announcements:[]});
  const duplicated={...wire(rooms,{}),account_data:{events:[{type:"m.direct",content:{}},{type:"m.direct",content:{}}]}};
  assert.equal(decisionRoomsSnapshot(duplicated,Object.keys(rooms)),null);
  assert.equal(decisionRoomsSnapshot({},[]),null);
});
