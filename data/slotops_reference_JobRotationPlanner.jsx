import { useState, useEffect, useRef, useCallback, useMemo } from "react";

const CC_UTC = {
  US:-5,CA:-5,MX:-6,GT:-6,BZ:-6,HN:-6,SV:-6,NI:-6,CR:-6,PA:-5,CU:-5,JM:-5,HT:-5,DO:-4,TT:-4,BB:-4,GY:-4,SR:-3,BR:-3,AR:-3,UY:-3,CL:-3,BO:-4,PY:-4,PE:-5,EC:-5,CO:-5,VE:-4,
  GB:0,IE:0,PT:0,IS:0,FR:1,DE:1,NL:1,BE:1,LU:1,CH:1,AT:1,ES:1,IT:1,NO:1,SE:1,DK:1,FI:2,PL:1,CZ:1,SK:1,HU:1,HR:1,SI:1,RS:1,ME:1,BA:1,AL:1,MK:1,GR:2,BG:2,RO:2,MD:2,UA:2,BY:3,EE:2,LV:2,LT:2,RU:3,
  TR:3,IL:2,JO:2,LB:2,SY:2,IQ:3,IR:3.5,SA:3,KW:3,QA:3,BH:3,AE:4,OM:4,YE:3,PS:2,
  EG:2,LY:2,TN:1,DZ:1,MA:1,SD:3,ET:3,KE:3,TZ:3,UG:3,RW:2,BI:2,SO:3,MG:3,MW:2,MZ:2,ZM:2,ZW:2,BW:2,NA:2,LS:2,SZ:2,ZA:2,NG:1,GH:0,SN:0,CI:0,ML:0,MR:0,GM:0,GN:0,SL:0,LR:0,TG:0,BJ:1,NE:1,BF:0,CM:1,CF:1,TD:1,GA:1,CG:1,CD:1,AO:1,MU:4,SC:4,
  IN:5.5,LK:5.5,NP:5.75,BD:6,BT:6,PK:5,AF:4.5,MV:5,MM:6.5,TH:7,VN:7,LA:7,KH:7,ID:7,MY:8,SG:8,PH:8,CN:8,TW:8,HK:8,BN:8,JP:9,KR:9,TL:9,MN:8,KZ:6,UZ:5,TJ:5,TM:5,KG:6,AM:4,AZ:4,GE:4,
  AU:10,NZ:12,PG:10,FJ:12,WS:13,TO:13,VU:11,SB:11,PW:9,MH:12,FM:11,NR:12,KI:14,TV:12,CK:-10,NU:-11,
};
function getUTC(c){const u=CC_UTC[(c||"US").toUpperCase().trim()];return u!==undefined?u:-5;}
function utcToIST(u){let h=15.5-u;if(h>=24)h-=24;if(h<0)h+=24;return h;}

function buildRotation(jobs, days=30){
  const byKey={};
  for(const j of jobs){
    const k=`${j.client}||${j.country}`;
    if(!byKey[k])byKey[k]={client:j.client,country:j.country,isScale:j.isScale,jobs:[]};
    byKey[k].jobs.push(j);
  }
  const batches=Object.values(byKey).sort((a,b)=>{
    if(a.isScale&&!b.isScale)return -1;
    if(!a.isScale&&b.isScale)return 1;
    return a.client.localeCompare(b.client)||a.country.localeCompare(b.country);
  });
  const total=jobs.length||1, SLOTS=501;
  const bSlots=batches.map(b=>({...b,slots:Math.max(1,Math.min(Math.round((b.jobs.length/total)*SLOTS),b.jobs.length))}));

  return Array.from({length:days},(_,di)=>{
    const day=di+1, date=new Date(); date.setDate(date.getDate()+di);
    const dow=date.getDay(), isWknd=dow===0||dow===6, isPrime=dow>=2&&dow<=4;
    const label=date.toLocaleDateString("en-GB",{weekday:"long",day:"2-digit",month:"short"});
    const todaySet=new Set(), prevSet=new Set();
    for(const b of bSlots){
      const n=b.slots, len=b.jobs.length, step=Math.max(1,Math.floor(n*0.4));
      const off=((day-1)*step)%len, poff=((day-2)*step+len)%len;
      for(let i=0;i<n;i++){todaySet.add(b.jobs[(off+i)%len].id);}
      for(let i=0;i<n;i++){prevSet.add(b.jobs[(poff+i)%len].id);}
    }
    const goLive=jobs.filter(j=>todaySet.has(j.id)&&!prevSet.has(j.id));
    const takeDown=jobs.filter(j=>prevSet.has(j.id)&&!todaySet.has(j.id));
    const keepLive=jobs.filter(j=>todaySet.has(j.id)&&prevSet.has(j.id));
    return{day,label,isWknd,isPrime,goLive,takeDown,keepLive,totalLive:todaySet.size};
  });
}

const CP=["#3b82f6","#f59e0b","#10b981","#8b5cf6","#ef4444","#ec4899","#06b6d4","#84cc16","#f97316","#6366f1"];
const ccM={};let pi=0;
function cc(n){if(!n)return"#94a3b8";if(!ccM[n])ccM[n]=CP[pi++%CP.length];return ccM[n];}

export default function App(){
  const [theme,setTheme]=useState("dark");
  const [ready,setReady]=useState(false);
  const [step,setStep]=useState("upload");
  const [msg,setMsg]=useState("");
  const [rotation,setRotation]=useState([]);
  const [day,setDay]=useState(1);
  const [done,setDone]=useState({});
  const [err,setErr]=useState("");
  const [showKeep,setShowKeep]=useState(false);
  const fileRef=useRef();
  const D=theme==="dark";

  // colours
  const bg    = D?"#0d1117":"#f6f8fa";
  const surf  = D?"#161b22":"#ffffff";
  const bord  = D?"#30363d":"#d0d7de";
  const txt   = D?"#e6edf3":"#1f2328";
  const muted = D?"#8b949e":"#656d76";
  const faint = D?"#21262d":"#f3f4f6";

  useEffect(()=>{
    if(window.XLSX){setReady(true);return;}
    const load=(src,fb)=>{
      const s=document.createElement("script");
      s.src=src;s.onload=()=>setReady(true);
      s.onerror=()=>fb?load(fb,null):setErr("XLSX engine failed. Refresh.");
      document.head.appendChild(s);
    };
    load("https://cdnjs.cloudflare.com/ajax/libs/xlsx/0.18.5/xlsx.full.min.js","https://unpkg.com/xlsx@0.18.5/dist/xlsx.full.min.js");
  },[]);

  const processFile=useCallback(async(file)=>{
    if(!window.XLSX){setErr("Still loading, wait 2 seconds.");return;}
    setStep("loading");setErr("");setMsg("Reading file...");
    await new Promise(r=>setTimeout(r,80));
    try{
      const buf=await file.arrayBuffer();
      const wb=window.XLSX.read(buf,{type:"array"});
      const allJobs=[];
      setMsg("Parsing jobs...");await new Promise(r=>setTimeout(r,150));
      wb.SheetNames.forEach(sheet=>{
        const di=sheet.lastIndexOf(" - ");
        const client=di>=0?sheet.slice(0,di).trim():sheet.trim();
        const rows=window.XLSX.utils.sheet_to_json(wb.Sheets[sheet],{header:1,defval:null})
          .slice(1).filter(r=>r&&(r[1]||r[4])&&String(r[1]||r[4]).trim());
        if(!rows.length)return;
        rows.forEach((row,i)=>{
          const cc2=(row[9]||"US").toString().toUpperCase().trim();
          const isScale=client.toLowerCase().includes("scale");
          const refNum=row[11]!=null?String(row[11]).replace(/\.0$/,""):"";
          const projectName=row[1]?String(row[1]).trim():(refNum||`Job ${i+1}`);
          const city=row[7]||"", state=row[8]||"";
          const loc=[city,state,cc2!=="US"?cc2:""].filter(Boolean).join(", ");
          allJobs.push({
            id:`${client}-${i}-${refNum}`,
            client, projectName,
            title: row[4]||"",
            country: cc2, location: loc,
            isScale, priority: isScale?100:50,
            ist: utcToIST(getUTC(cc2)),
          });
        });
      });
      if(!allJobs.length){setErr("No jobs found. Check your file.");setStep("upload");return;}
      setMsg("Building rotation...");await new Promise(r=>setTimeout(r,200));
      setRotation(buildRotation(allJobs,30));
      setDay(1);setDone({});setStep("plan");
    }catch(e){
      console.error(e);setErr(`Error: ${e.message}`);setStep("upload");
    }
  },[]);

  const handleFile=useCallback(f=>{if(f)processFile(f);},[processFile]);
  const toggle=useCallback((id)=>setDone(d=>({...d,[id]:!d[id]})),[]);

  const plan=rotation[day-1];
  const tomorrow=rotation[day];

  const doneCt=useMemo(()=>Object.values(done).filter(Boolean).length,[done]);
  const total=(plan?.goLive.length||0)+(plan?.takeDown.length||0);
  const pct=total>0?Math.round((doneCt/total)*100):0;

  // Section: a flat list of job rows
  const Section=({jobs,type})=>{
    if(!jobs||!jobs.length)return null;
    const isDown=type==="down";
    const color=isDown?"#f85149":"#3fb950";
    const label=isDown?"TAKE DOWN":"GO LIVE";
    const subLabel=isDown?"Remove these from LinkedIn":"Post these on LinkedIn";

    // group by client just for a divider, but jobs are always flat+visible
    const byClient={};
    for(const j of jobs){
      if(!byClient[j.client])byClient[j.client]=[];
      byClient[j.client].push(j);
    }
    // sort: Scale first
    const clientOrder=Object.keys(byClient).sort((a,b)=>{
      const as=a.toLowerCase().includes("scale"), bs=b.toLowerCase().includes("scale");
      if(as&&!bs)return -1;if(!as&&bs)return 1;return a.localeCompare(b);
    });

    return(
      <div style={{marginBottom:32}}>
        {/* Section header */}
        <div style={{display:"flex",alignItems:"center",gap:12,marginBottom:16}}>
          <div style={{width:4,height:24,borderRadius:2,background:color}}/>
          <div>
            <div style={{fontSize:18,fontWeight:700,color,letterSpacing:"-0.01em"}}>{label}</div>
            <div style={{fontSize:12,color:muted}}>{subLabel} · {jobs.length} jobs</div>
          </div>
          <div style={{flex:1,height:1,background:bord,marginLeft:4}}/>
        </div>

        {/* Jobs grouped by client */}
        {clientOrder.map(client=>{
          const cjobs=byClient[client];
          const clientDone=cjobs.every(j=>done[j.id]);
          const cColor=cc(client);
          const isScale=cjobs[0]?.isScale;
          return(
            <div key={client} style={{marginBottom:14}}>
              {/* Client label */}
              <div style={{display:"flex",alignItems:"center",gap:8,marginBottom:6,paddingLeft:2}}>
                <div style={{width:8,height:8,borderRadius:"50%",background:cColor,flexShrink:0}}/>
                <span style={{fontSize:12,fontWeight:600,color:cColor}}>{client}</span>
                {isScale&&<span style={{fontSize:10,color:"#f59e0b",background:"#f59e0b15",padding:"1px 6px",borderRadius:4,border:"1px solid #f59e0b30",fontWeight:600}}>★ PRIORITY</span>}
                <span style={{fontSize:11,color:muted}}>· {cjobs[0]?.country}</span>
                {clientDone&&<span style={{fontSize:11,color:"#3fb950",marginLeft:"auto"}}>✓ Done</span>}
              </div>

              {/* Job rows */}
              <div style={{border:`1px solid ${bord}`,borderRadius:10,overflow:"hidden",background:surf}}>
                {cjobs.map((j,idx)=>{
                  const isDone=!!done[j.id];
                  return(
                    <div key={j.id} onClick={()=>toggle(j.id)}
                      style={{
                        display:"flex",alignItems:"center",gap:12,
                        padding:"11px 16px",cursor:"pointer",
                        background:isDone?(D?"#161b22":"#f6f8fa"):surf,
                        borderBottom:idx<cjobs.length-1?`1px solid ${bord}`:"none",
                        transition:"background .1s",
                      }}>
                      {/* Checkbox */}
                      <div style={{
                        width:18,height:18,borderRadius:4,flexShrink:0,
                        border:`2px solid ${isDone?"#3fb950":D?"#30363d":"#d0d7de"}`,
                        background:isDone?"#3fb950":"transparent",
                        display:"flex",alignItems:"center",justifyContent:"center",
                        transition:"all .15s",
                      }}>
                        {isDone&&<span style={{color:"#fff",fontSize:11,fontWeight:700,lineHeight:1}}>✓</span>}
                      </div>

                      {/* Project name — ONLY THING SHOWN */}
                      <div style={{flex:1,minWidth:0}}>
                        <div style={{
                          fontSize:14,fontWeight:700,
                          color:isDone?muted:txt,
                          textDecoration:isDone?"line-through":"none",
                          letterSpacing:"-0.01em",
                          overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap",
                        }}>
                          {j.projectName}
                        </div>
                      </div>

                      {/* Location */}
                      {j.location&&(
                        <span style={{fontSize:11,color:muted,flexShrink:0}}>
                          {j.location}
                        </span>
                      )}
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    );
  };

  return(
    <div style={{background:bg,minHeight:"100vh",color:txt,fontFamily:"-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif",fontSize:14}}>
      <style>{`
        *{box-sizing:border-box;margin:0;padding:0}
        ::-webkit-scrollbar{width:6px}
        ::-webkit-scrollbar-thumb{background:${D?"#30363d":"#d0d7de"};border-radius:3px}
        button{cursor:pointer;font-family:inherit}
        .dz{transition:border-color .15s}
        .dz:hover,.dz.over{border-color:#2f81f7!important}
        .daybtn{transition:all .1s;border:none;background:transparent}
        .daybtn:hover{background:${D?"#21262d":"#f3f4f6"}!important}
        .daybtn.active{background:${D?"#1f6feb":"#dbeafe"}!important;color:${D?"#fff":"#1d4ed8"}!important;font-weight:700}
        @keyframes spin{to{transform:rotate(360deg)}}
      `}</style>

      {/* ── Header ── */}
      <div style={{background:surf,borderBottom:`1px solid ${bord}`,padding:"10px 20px",display:"flex",alignItems:"center",justifyContent:"space-between",position:"sticky",top:0,zIndex:50,gap:12}}>
        <div style={{display:"flex",alignItems:"center",gap:10,flex:1,minWidth:0}}>
          <div style={{fontWeight:700,fontSize:15,color:txt,flexShrink:0}}>SlotOps</div>
          {step==="plan"&&(
            <div style={{display:"flex",gap:2,overflowX:"auto",flex:1}}>
              {rotation.map((r,i)=>(
                <button key={i} className={`daybtn${day===i+1?" active":""}`}
                  onClick={()=>{setDay(i+1);setDone({});setShowKeep(false);}}
                  style={{padding:"5px 10px",borderRadius:6,color:r.isWknd?muted:txt,fontSize:12,whiteSpace:"nowrap",opacity:r.isWknd?0.4:1,flexShrink:0}}>
                  {r.isPrime&&!r.isWknd?"★ ":""}{r.label.split(",")[0]}
                </button>
              ))}
            </div>
          )}
        </div>
        <div style={{display:"flex",gap:8,flexShrink:0}}>
          {step==="plan"&&(
            <button onClick={()=>{setStep("upload");setRotation([]);setDone({});Object.keys(ccM).forEach(k=>delete ccM[k]);pi=0;}}
              style={{padding:"5px 12px",borderRadius:6,border:`1px solid ${bord}`,background:surf,color:muted,fontSize:12}}>
              ↑ New File
            </button>
          )}
          <button onClick={()=>setTheme(t=>t==="dark"?"light":"dark")}
            style={{width:64,height:28,borderRadius:14,border:`1px solid ${bord}`,background:faint,position:"relative",display:"flex",alignItems:"center",padding:"0 3px"}}>
            <div style={{width:22,height:22,borderRadius:11,background:D?"#6366f1":"#f59e0b",display:"flex",alignItems:"center",justifyContent:"center",fontSize:13,transform:D?"translateX(0)":"translateX(32px)",transition:"transform .2s, background .2s"}}>
              {D?"🌙":"☀️"}
            </div>
          </button>
        </div>
      </div>

      {/* ── Upload ── */}
      {step==="upload"&&(
        <div style={{display:"flex",alignItems:"center",justifyContent:"center",minHeight:"calc(100vh - 49px)",padding:32}}>
          <div style={{maxWidth:440,width:"100%",textAlign:"center"}}>
            <div style={{fontSize:26,fontWeight:700,color:txt,marginBottom:8}}>Job Rotation Planner</div>
            <div style={{fontSize:14,color:muted,marginBottom:24,lineHeight:1.6}}>Upload your XLSX → see which jobs to take down and go live each day</div>
            <div style={{height:22,marginBottom:14}}>
              {err?<span style={{fontSize:12,color:"#f85149"}}>{err}</span>
              :ready?<span style={{fontSize:12,color:"#3fb950"}}>✓ Ready — drop your file</span>
              :<span style={{fontSize:12,color:muted}}>Loading XLSX engine…</span>}
            </div>
            <div className="dz" onClick={()=>ready&&fileRef.current?.click()}
              onDrop={e=>{e.preventDefault();e.currentTarget.classList.remove("over");if(ready)handleFile(e.dataTransfer.files[0]);}}
              onDragOver={e=>{e.preventDefault();e.currentTarget.classList.add("over");}}
              onDragLeave={e=>e.currentTarget.classList.remove("over")}
              style={{border:`2px dashed ${bord}`,borderRadius:12,padding:"44px 28px",cursor:ready?"pointer":"default",background:surf,marginBottom:16,opacity:ready?1:0.5}}>
              <div style={{fontSize:44,marginBottom:10}}>📊</div>
              <div style={{fontSize:15,fontWeight:600,color:txt,marginBottom:4}}>Drop .xlsx here or click to browse</div>
              <div style={{fontSize:12,color:muted}}>One tab per client · Column B = Project Name</div>
              <input ref={fileRef} type="file" accept=".xlsx,.xls" style={{display:"none"}} onChange={e=>{if(e.target.files[0])handleFile(e.target.files[0]);e.target.value="";}}/>
            </div>
          </div>
        </div>
      )}

      {/* ── Loading ── */}
      {step==="loading"&&(
        <div style={{display:"flex",flexDirection:"column",alignItems:"center",justifyContent:"center",minHeight:"calc(100vh - 49px)",gap:12}}>
          <div style={{fontSize:30,animation:"spin 1s linear infinite"}}>↻</div>
          <div style={{fontSize:13,color:muted}}>{msg}</div>
        </div>
      )}

      {/* ── Plan ── */}
      {step==="plan"&&plan&&(
        <div style={{maxWidth:780,margin:"0 auto",padding:"28px 20px"}}>

          {/* Day summary */}
          <div style={{display:"flex",alignItems:"center",justifyContent:"space-between",marginBottom:20}}>
            <div>
              <div style={{fontSize:22,fontWeight:700,color:txt,letterSpacing:"-0.02em"}}>
                {plan.label}
                {plan.isPrime&&!plan.isWknd&&<span style={{fontSize:12,color:"#f59e0b",marginLeft:10,fontWeight:600,background:"#f59e0b15",padding:"2px 8px",borderRadius:10}}>★ Prime day</span>}
                {plan.isWknd&&<span style={{fontSize:12,color:muted,marginLeft:10,background:faint,padding:"2px 8px",borderRadius:10}}>Weekend</span>}
              </div>
              <div style={{fontSize:13,color:muted,marginTop:4}}>
                {plan.takeDown.length} to take down · {plan.goLive.length} to go live · {plan.totalLive} total live
              </div>
            </div>
            {total>0&&(
              <div style={{textAlign:"right"}}>
                <div style={{fontSize:26,fontWeight:700,color:pct===100?"#3fb950":txt}}>{pct}%</div>
                <div style={{fontSize:11,color:muted}}>{doneCt} / {total} done</div>
              </div>
            )}
          </div>

          {/* Progress bar */}
          {total>0&&(
            <div style={{height:4,background:D?"#21262d":"#e2e8f0",borderRadius:2,marginBottom:28,overflow:"hidden"}}>
              <div style={{height:"100%",width:`${pct}%`,background:pct===100?"#3fb950":"#2f81f7",borderRadius:2,transition:"width .3s"}}/>
            </div>
          )}

          {plan.takeDown.length===0&&plan.goLive.length===0&&(
            <div style={{textAlign:"center",padding:48,color:muted,fontSize:14,background:surf,borderRadius:12,border:`1px solid ${bord}`}}>
              Nothing changes today — all {plan.totalLive} jobs stay live as-is.
            </div>
          )}

          {/* TAKE DOWN */}
          <Section jobs={plan.takeDown} type="down" />

          {/* GO LIVE */}
          <Section jobs={plan.goLive} type="up" />

          {/* KEEP LIVE toggle */}
          {plan.keepLive.length>0&&(
            <div style={{marginBottom:28}}>
              <button onClick={()=>setShowKeep(s=>!s)}
                style={{width:"100%",padding:"10px 16px",border:`1px solid ${bord}`,borderRadius:10,background:surf,color:muted,fontSize:13,display:"flex",alignItems:"center",justifyContent:"space-between"}}>
                <span>{showKeep?"▾":"▸"} No change needed ({plan.keepLive.length} jobs staying live)</span>
              </button>
              {showKeep&&(
                <div style={{border:`1px solid ${bord}`,borderTop:"none",borderRadius:"0 0 10px 10px",background:surf,overflow:"hidden"}}>
                  {plan.keepLive.map((j,i)=>(
                    <div key={j.id} style={{display:"flex",alignItems:"center",gap:12,padding:"10px 16px",borderBottom:i<plan.keepLive.length-1?`1px solid ${bord}`:"none"}}>
                      <div style={{width:8,height:8,borderRadius:"50%",background:cc(j.client),flexShrink:0}}/>
                      <div style={{flex:1,minWidth:0}}>
                        <div style={{fontSize:13,fontWeight:600,color:muted,overflow:"hidden",textOverflow:"ellipsis",whiteSpace:"nowrap"}}>{j.projectName}</div>
                      </div>
                      <span style={{fontSize:11,color:muted,flexShrink:0}}>{j.client} · {j.country}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          {/* Tomorrow preview */}
          {tomorrow&&(tomorrow.takeDown.length>0||tomorrow.goLive.length>0)&&(
            <div style={{background:surf,border:`1px solid ${bord}`,borderRadius:12,padding:18}}>
              <div style={{fontSize:13,fontWeight:600,color:muted,marginBottom:14}}>
                Tomorrow — {tomorrow.label}
                {tomorrow.isPrime&&!tomorrow.isWknd&&<span style={{color:"#f59e0b",marginLeft:8,fontSize:11}}>★ Prime</span>}
              </div>
              <div style={{display:"grid",gridTemplateColumns:"1fr 1fr",gap:16}}>
                <div>
                  <div style={{fontSize:12,fontWeight:600,color:"#f85149",marginBottom:8}}>Will take down · {tomorrow.takeDown.length}</div>
                  {Object.entries(tomorrow.takeDown.reduce((m,j)=>{(m[j.client]=m[j.client]||[]).push(j);return m;},{})).map(([c,js])=>(
                    <div key={c} style={{padding:"6px 10px",background:D?"#1f0808":"#fff1f2",border:"1px solid #f8514930",borderRadius:7,marginBottom:5,fontSize:13,color:txt,fontWeight:500}}>
                      {c} <span style={{color:muted,fontWeight:400,fontSize:11}}>· {js[0].country} · {js.length} jobs</span>
                    </div>
                  ))}
                </div>
                <div>
                  <div style={{fontSize:12,fontWeight:600,color:"#3fb950",marginBottom:8}}>Will go live · {tomorrow.goLive.length}</div>
                  {Object.entries(tomorrow.goLive.reduce((m,j)=>{(m[j.client]=m[j.client]||[]).push(j);return m;},{})).map(([c,js])=>(
                    <div key={c} style={{padding:"6px 10px",background:D?"#052010":"#f0fdf4",border:"1px solid #3fb95030",borderRadius:7,marginBottom:5,fontSize:13,color:txt,fontWeight:500}}>
                      {c} <span style={{color:muted,fontWeight:400,fontSize:11}}>· {js[0].country} · {js.length} jobs</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
