import { object } from "./pcp-sync-observation.js";

export const decisionRoomsFilter = {
  room:{include_leave:false,timeline:{limit:0,types:[]},state:{types:["m.room.create","m.room.name","m.space.child"]},
    ephemeral:{types:[]},account_data:{types:[]}},
  presence:{types:[]},account_data:{types:["m.direct"]},
};
export type DecisionRoomsSnapshot = {
  schema: "matrix-decision-rooms-v1";
  observed_at: number;
  direct: string[];
  announcements: string[];
};
const roomId = (value: unknown): value is string => typeof value === "string" &&
  /^![^\s\u0000-\u001f\u007f]{1,254}$/.test(value);

function events(value:unknown):Record<string,any>[]|null{
  return Array.isArray(value)&&value.every(object)?value:null;
}

/** Only server state from the account's authenticated, filtered initial
 * snapshot. Room coverage must agree with the native joined-room inventory.
 * No SDK display name fallbacks, message content or member counts. */
export function decisionRoomsSnapshot(value:unknown, joined:unknown, now=Date.now()): DecisionRoomsSnapshot|null{
  if(!object(value)||typeof value.next_batch!=="string"||!value.next_batch||!Array.isArray(joined)||!joined.every(roomId)||
      value.account_data!==undefined&&!object(value.account_data)||value.rooms!==undefined&&!object(value.rooms))return null;
  const observedRooms=value.rooms?.join===undefined?{}:value.rooms.join;
  if(!object(observedRooms))return null;
  const roomIds=Object.keys(observedRooms),expected=new Set(joined);
  if(expected.size!==roomIds.length||!roomIds.every(id=>expected.has(id)))return null;
  const accountEvents=events(value.account_data?.events===undefined?[]:value.account_data.events);
  const directEvents=accountEvents?.filter(event=>event.type==="m.direct");
  if(!directEvents||directEvents.length>1||directEvents.length===1&&!object(directEvents[0].content))return null;
  const direct=new Set<string>(),announcements=new Set<string>();
  for(const ids of Object.values(directEvents[0]?.content??{})){
    if(!Array.isArray(ids)||!ids.every(roomId))return null;
    for(const id of ids)if(expected.has(id))direct.add(id);
  }
  const rooms=new Map<string,{space:boolean,name:unknown,children:string[]}>();
  for(const id of roomIds){
    const info=observedRooms[id];
    if(!object(info)||!object(info.state))return null;
    const state=events(info.state.events);
    if(!state)return null;
    const create=state.filter(e=>e.type==="m.room.create"&&e.state_key==="");
    if(create.length!==1||!object(create[0].content))return null;
    const names=state.filter(e=>e.type==="m.room.name"&&e.state_key==="");
    if(names.length>1)return null;
    const children=state.filter(e=>e.type==="m.space.child"&&roomId(e.state_key)&&
      object(e.content)&&Array.isArray(e.content.via)&&e.content.via.some(server=>typeof server==="string"&&server.length>0));
    rooms.set(id,{space:create[0].content.type==="m.space",name:names[0]?.content?.name,children:children.map(e=>e.state_key)});
  }
  for(const space of rooms.values())if(space.space){
    for(const id of space.children){
      const child=rooms.get(id);
      if(child&&!child.space&&typeof child.name==="string"&&child.name.trim().toLowerCase()==="announcements")announcements.add(id);
    }
  }
  return {schema:"matrix-decision-rooms-v1",observed_at:now,direct:[...direct].sort(),announcements:[...announcements].sort()};
}
