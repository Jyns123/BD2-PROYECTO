import { useState, useEffect, useRef, useCallback } from 'react';
import { Maximize2, Minimize2, AlertCircle, CheckCircle2, Info } from 'lucide-react';
import Sidebar from './components/Sidebar';
import QueryEditor from './components/QueryEditor';
import ResultsPanel from './components/ResultsPanel';
import StatsBar from './components/StatsBar';
import CreateTableModal from './components/CreateTableModal';
import { getTables, getTable, runQuery, dropTable } from './services/api';

// ─────────────────────────────────────────────────────────────────────────────
// R-TREE VISUALIZER (QGIS-style)  — self-contained, mounted as a panel
// ─────────────────────────────────────────────────────────────────────────────

const API = 'http://localhost:8000';

const LEVEL_PALETTE = [
  { stroke: '#e05c5c', fill: 'rgba(224,92,92,0.08)',   label: 'Root'   },
  { stroke: '#e08c3c', fill: 'rgba(224,140,60,0.07)',  label: 'L1'     },
  { stroke: '#d4b84a', fill: 'rgba(212,184,74,0.08)',  label: 'L2'     },
  { stroke: '#5cb85c', fill: 'rgba(92,184,92,0.07)',   label: 'L3'     },
  { stroke: '#5bc0de', fill: 'rgba(91,192,222,0.07)',  label: 'L4'     },
  { stroke: '#8a6bbf', fill: 'rgba(138,107,191,0.07)', label: 'L5'     },
  { stroke: '#d45ca0', fill: 'rgba(212,92,160,0.07)',  label: 'L6'     },
];
const color = (lv) => LEVEL_PALETTE[lv % LEVEL_PALETTE.length];

// Build world→canvas transform with pan/zoom
function makeTransform(mbrs, W, H, panX, panY, zoom, padding = 48) {
  if (!mbrs.length) return null;
  const allX = mbrs.flatMap(m => [m.min_x, m.max_x]);
  const allY = mbrs.flatMap(m => [m.min_y, m.max_y]);
  const dataMinX = Math.min(...allX), dataMaxX = Math.max(...allX);
  const dataMinY = Math.min(...allY), dataMaxY = Math.max(...allY);
  const dataW = dataMaxX - dataMinX || 1;
  const dataH = dataMaxY - dataMinY || 1;
  const baseScale = Math.min((W - padding * 2) / dataW, (H - padding * 2) / dataH);
  const scale = baseScale * zoom;
  // Center the data in canvas, then apply pan
  const cx = W / 2 + panX - (dataMinX + dataW / 2) * scale;
  const cy = H / 2 + panY + (dataMinY + dataH / 2) * scale; // flip Y

  return {
    toCanvasX: x => cx + x * scale,
    toCanvasY: y => cy - y * scale,         // Y flip: world up = canvas up
    toWorldX:  px => (px - cx) / scale,
    toWorldY:  py => -(py - cy) / scale,
    scale,
    dataMinX, dataMaxX, dataMinY, dataMaxY,
  };
}

function drawGrid(ctx, W, H, tr) {
  if (!tr) return;
  // Draw graticule lines in world space
  const worldL = tr.toWorldX(0), worldR = tr.toWorldX(W);
  const worldB = tr.toWorldY(H), worldT = tr.toWorldY(0);
  const span = Math.max(worldR - worldL, worldT - worldB);
  const rawStep = span / 6;
  const mag = Math.pow(10, Math.floor(Math.log10(rawStep)));
  const nice = [1, 2, 5, 10].map(f => f * mag).find(s => span / s <= 8) || mag;

  ctx.strokeStyle = 'rgba(255,255,255,0.04)';
  ctx.lineWidth = 1;
  ctx.setLineDash([]);

  const startX = Math.ceil(worldL / nice) * nice;
  for (let wx = startX; wx <= worldR + nice; wx += nice) {
    const cx2 = tr.toCanvasX(wx);
    ctx.beginPath(); ctx.moveTo(cx2, 0); ctx.lineTo(cx2, H); ctx.stroke();
  }
  const startY = Math.ceil(worldB / nice) * nice;
  for (let wy = startY; wy <= worldT + nice; wy += nice) {
    const cy2 = tr.toCanvasY(wy);
    ctx.beginPath(); ctx.moveTo(0, cy2); ctx.lineTo(W, cy2); ctx.stroke();
  }

  // Axis labels
  ctx.fillStyle = 'rgba(255,255,255,0.18)';
  ctx.font = '10px monospace';
  for (let wx = startX; wx <= worldR + nice; wx += nice) {
    const cx2 = tr.toCanvasX(wx);
    if (cx2 > 30 && cx2 < W - 10) ctx.fillText(wx.toFixed(2), cx2 + 2, H - 6);
  }
  for (let wy = startY; wy <= worldT + nice; wy += nice) {
    const cy2 = tr.toCanvasY(wy);
    if (cy2 > 10 && cy2 < H - 20) ctx.fillText(wy.toFixed(2), 4, cy2 - 2);
  }
}

function drawMBRs(ctx, mbrs, visible, showLeaves, tr, hoveredIdx) {
  if (!tr) return;
  const sorted = mbrs
    .map((m, i) => ({ ...m, _i: i }))
    .filter(m => visible.has(m.level) && (showLeaves || !m.is_leaf))
    .sort((a, b) => a.level - b.level);

  for (const m of sorted) {
    const { stroke, fill } = color(m.level);
    const x1 = tr.toCanvasX(m.min_x), y1 = tr.toCanvasY(m.max_y);
    const x2 = tr.toCanvasX(m.max_x), y2 = tr.toCanvasY(m.min_y);
    const w = x2 - x1, h = y2 - y1;
    const hov = m._i === hoveredIdx;

    ctx.fillStyle = hov ? stroke + '28' : fill;
    ctx.fillRect(x1, y1, w, h);

    ctx.strokeStyle = hov ? stroke : stroke + 'cc';
    ctx.lineWidth   = hov ? 2 : m.is_leaf ? 0.8 : 1.4;
    ctx.setLineDash(m.is_leaf ? [] : [5, 3]);
    ctx.strokeRect(x1, y1, w, h);
    ctx.setLineDash([]);

    // Corner label for internal nodes
    if (!m.is_leaf && w > 50 && h > 18) {
      ctx.font      = `bold 9px monospace`;
      ctx.fillStyle = stroke + 'cc';
      ctx.fillText(`L${m.level}  (${m.n_entries})`, x1 + 4, y1 + 12);
    }

    // Point dot when leaf is tiny
    if (m.is_leaf && w < 3 && h < 3) {
      ctx.beginPath();
      ctx.arc(x1 + w / 2, y1 + h / 2, hov ? 5 : 3, 0, Math.PI * 2);
      ctx.fillStyle = stroke;
      ctx.fill();
    }
  }
}

function hitTest(mbrs, visible, showLeaves, tr, mx, my) {
  if (!tr) return -1;
  const cands = mbrs
    .map((m, i) => ({ ...m, _i: i }))
    .filter(m => visible.has(m.level) && (showLeaves || !m.is_leaf))
    .reverse();
  for (const m of cands) {
    const x1 = tr.toCanvasX(m.min_x), y1 = tr.toCanvasY(m.max_y);
    const x2 = tr.toCanvasX(m.max_x), y2 = tr.toCanvasY(m.min_y);
    if (mx >= x1 && mx <= x2 && my >= y1 && my <= y2) return m._i;
  }
  return -1;
}

function RTreeViewer({ tableName }) {
  const canvasRef   = useRef(null);
  const trRef       = useRef(null);
  const [data, setData]           = useState(null);
  const [loading, setLoading]     = useState(false);
  const [err, setErr]             = useState(null);
  const [visible, setVisible]     = useState(new Set());
  const [showLeaves, setLeaves]   = useState(true);
  const [hovered, setHovered]     = useState(-1);
  const [tooltip, setTooltip]     = useState(null);
  const [cursor, setCursor]       = useState({ wx: 0, wy: 0 });
  const [selected, setSelected]   = useState(null);
  // pan / zoom state
  const panRef   = useRef({ x: 0, y: 0 });
  const zoomRef  = useRef(1);
  const dragRef  = useRef(null);
  const [, forceRedraw] = useState(0);

  const redraw = useCallback(() => forceRedraw(n => n + 1), []);

  // Load
  useEffect(() => {
    if (!tableName) return;
    setLoading(true); setErr(null); setData(null); setHovered(-1); setTooltip(null);
    setSelected(null); panRef.current = { x: 0, y: 0 }; zoomRef.current = 1;
    fetch(`${API}/rtree-mbrs/${tableName}`)
      .then(r => r.ok ? r.json() : r.json().then(j => { throw new Error(j.detail); }))
      .then(d => {
        setData(d);
        setVisible(new Set(d.mbrs.map(m => m.level)));
      })
      .catch(e => setErr(e.message))
      .finally(() => setLoading(false));
  }, [tableName]);

  // Render
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !data) return;
    const W = canvas.width, H = canvas.height;
    const ctx = canvas.getContext('2d');
    ctx.clearRect(0, 0, W, H);

    // Background
    ctx.fillStyle = '#111318';
    ctx.fillRect(0, 0, W, H);

    const tr = makeTransform(
      data.mbrs.filter(m => visible.has(m.level)),
      W, H, panRef.current.x, panRef.current.y, zoomRef.current
    );
    trRef.current = tr;

    drawGrid(ctx, W, H, tr);
    drawMBRs(ctx, data.mbrs, visible, showLeaves, tr, hovered);

    // Scale bar (bottom-left)
    if (tr) {
      const barWorldLen = (tr.dataMaxX - tr.dataMinX) / 6;
      const barPx = barWorldLen * tr.scale;
      const bx = 16, by = H - 22;
      ctx.strokeStyle = '#aaa'; ctx.lineWidth = 2; ctx.setLineDash([]);
      ctx.beginPath(); ctx.moveTo(bx, by); ctx.lineTo(bx + barPx, by);
      ctx.moveTo(bx, by - 4); ctx.lineTo(bx, by + 4);
      ctx.moveTo(bx + barPx, by - 4); ctx.lineTo(bx + barPx, by + 4);
      ctx.stroke();
      ctx.fillStyle = '#aaa'; ctx.font = '10px monospace';
      ctx.fillText(barWorldLen.toFixed(2) + ' u', bx + barPx / 2 - 18, by - 6);
    }

    // Page count watermark (top-right)
    ctx.fillStyle = 'rgba(255,255,255,0.07)';
    ctx.font = 'bold 11px monospace';
    if (data) ctx.fillText(`${data.total_nodes} nodes · ${data.n_points} pts`, W - 180, 20);
  }, [data, visible, showLeaves, hovered]);

  // Mouse handlers
  const onMouseMove = useCallback(e => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const r = canvas.getBoundingClientRect();
    const mx = (e.clientX - r.left) * (canvas.width / r.width);
    const my = (e.clientY - r.top) * (canvas.height / r.height);

    // Pan drag
    if (dragRef.current) {
      panRef.current = {
        x: panRef.current.x + (e.clientX - dragRef.current.x),
        y: panRef.current.y + (e.clientY - dragRef.current.y),
      };
      dragRef.current = { x: e.clientX, y: e.clientY };
      redraw();
      return;
    }

    if (trRef.current) {
      setCursor({ wx: trRef.current.toWorldX(mx), wy: trRef.current.toWorldY(my) });
    }

    if (!data) return;
    const idx = hitTest(data.mbrs, visible, showLeaves, trRef.current, mx, my);
    setHovered(idx);
    if (idx !== -1) {
      setTooltip({ x: e.clientX + 14, y: e.clientY - 10, mbr: data.mbrs[idx] });
    } else {
      setTooltip(null);
    }
  }, [data, visible, showLeaves, redraw]);

  const onMouseDown = useCallback(e => {
    if (e.button === 1 || e.altKey) {
      e.preventDefault();
      dragRef.current = { x: e.clientX, y: e.clientY };
    }
  }, []);

  const onMouseUp = useCallback(() => { dragRef.current = null; }, []);

  const onWheel = useCallback(e => {
    e.preventDefault();
    const factor = e.deltaY < 0 ? 1.12 : 1 / 1.12;
    zoomRef.current = Math.max(0.1, Math.min(50, zoomRef.current * factor));
    redraw();
  }, [redraw]);

  const onClick = useCallback(e => {
    if (!data) return;
    const canvas = canvasRef.current;
    const r = canvas.getBoundingClientRect();
    const mx = (e.clientX - r.left) * (canvas.width / r.width);
    const my = (e.clientY - r.top) * (canvas.height / r.height);
    const idx = hitTest(data.mbrs, visible, showLeaves, trRef.current, mx, my);
    setSelected(idx !== -1 ? data.mbrs[idx] : null);
  }, [data, visible, showLeaves]);

  const resetView = () => {
    panRef.current  = { x: 0, y: 0 };
    zoomRef.current = 1;
    redraw();
  };

  const allLevels = data ? [...new Set(data.mbrs.map(m => m.level))].sort() : [];

  return (
    <div style={{ display: 'flex', height: '100%', background: '#0f1015', fontFamily: 'monospace', color: '#cdd6f4', fontSize: 12 }}>

      {/* ── LAYERS PANEL (QGIS left) ── */}
      <div style={{ width: 220, background: '#13151c', borderRight: '1px solid #1e2030', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
        {/* Header */}
        <div style={{ padding: '10px 12px 6px', borderBottom: '1px solid #1e2030', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: 1, color: '#89b4fa', textTransform: 'uppercase' }}>Layers</span>
          <span style={{ fontSize: 10, color: '#45475a' }}>{tableName || '—'}</span>
        </div>

        {/* Layer list */}
        <div style={{ flex: 1, overflowY: 'auto', padding: '6px 8px' }}>
          {allLevels.map(lv => {
            const { stroke } = color(lv);
            const cnt   = data.mbrs.filter(m => m.level === lv).length;
            const on    = visible.has(lv);
            const isLeafLevel = data.mbrs.filter(m => m.level === lv).every(m => m.is_leaf);
            return (
              <div
                key={lv}
                onClick={() => setVisible(prev => { const n = new Set(prev); on ? n.delete(lv) : n.add(lv); return n; })}
                style={{
                  display: 'flex', alignItems: 'center', gap: 7, padding: '5px 6px', borderRadius: 4,
                  cursor: 'pointer', marginBottom: 2,
                  background: on ? '#1e2030' : 'transparent',
                  opacity: on ? 1 : 0.38,
                  transition: 'all 0.1s',
                }}
              >
                {/* Swatch */}
                <svg width={18} height={14} style={{ flexShrink: 0 }}>
                  <rect x={1} y={2} width={16} height={10} rx={1}
                    fill={stroke + '22'} stroke={stroke} strokeWidth={isLeafLevel ? 1 : 1.5}
                    strokeDasharray={isLeafLevel ? 'none' : '4 2'} />
                </svg>
                <span style={{ flex: 1, fontSize: 11, color: on ? '#cdd6f4' : '#585b70' }}>
                  Level {lv}{lv === 0 ? ' (root)' : ''}
                </span>
                <span style={{ fontSize: 10, color: '#45475a' }}>{cnt}</span>
              </div>
            );
          })}

          {/* Leaves toggle */}
          {data && (
            <div
              onClick={() => setLeaves(v => !v)}
              style={{
                display: 'flex', alignItems: 'center', gap: 7, padding: '5px 6px', borderRadius: 4,
                cursor: 'pointer', marginTop: 8, borderTop: '1px solid #1e2030', paddingTop: 10,
                opacity: showLeaves ? 1 : 0.4,
              }}
            >
              <svg width={18} height={14}>
                <circle cx={9} cy={7} r={4} fill='#a6e3a1' fillOpacity={0.3} stroke='#a6e3a1' strokeWidth={1} />
              </svg>
              <span style={{ fontSize: 11, color: showLeaves ? '#a6e3a1' : '#585b70' }}>Leaf nodes</span>
            </div>
          )}
        </div>

        {/* Quick actions */}
        <div style={{ padding: '8px', borderTop: '1px solid #1e2030', display: 'flex', gap: 4 }}>
          <QBtn onClick={() => setVisible(new Set(allLevels))}>All</QBtn>
          <QBtn onClick={() => setVisible(new Set())}>None</QBtn>
          <QBtn onClick={resetView} style={{ marginLeft: 'auto' }}>⌂ Fit</QBtn>
        </div>

        {/* Stats block */}
        {data && (
          <div style={{ padding: '8px 12px 10px', borderTop: '1px solid #1e2030', display: 'flex', flexDirection: 'column', gap: 4 }}>
            <StatRow k='Nodes'    v={data.total_nodes} />
            <StatRow k='Points'   v={data.n_points} />
            <StatRow k='Depth'    v={data.max_level} />
            <StatRow k='Leaves'   v={data.mbrs.filter(m => m.is_leaf).length} />
            <StatRow k='Internal' v={data.mbrs.filter(m => !m.is_leaf).length} />
          </div>
        )}
      </div>

      {/* ── MAP CANVAS ── */}
      <div style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
        <canvas
          ref={canvasRef}
          width={1400} height={900}
          onMouseMove={onMouseMove}
          onMouseDown={onMouseDown}
          onMouseUp={onMouseUp}
          onMouseLeave={() => { setHovered(-1); setTooltip(null); dragRef.current = null; }}
          onWheel={onWheel}
          onClick={onClick}
          style={{ width: '100%', height: '100%', display: 'block', cursor: dragRef.current ? 'grabbing' : hovered !== -1 ? 'pointer' : 'crosshair' }}
        />

        {/* Loading / error overlays */}
        {loading && <Overlay><Spin /> Loading R-Tree…</Overlay>}
        {err     && <Overlay color='#f38ba8'><AlertCircle size={14} /> {err}</Overlay>}
        {!data && !loading && !err && <Overlay color='#45475a'>Select a table with RTREE index</Overlay>}

        {/* Toolbar (top-right) */}
        <div style={{ position: 'absolute', top: 10, right: 10, display: 'flex', flexDirection: 'column', gap: 4 }}>
          {[['＋', () => { zoomRef.current = Math.min(50, zoomRef.current * 1.3); redraw(); }],
            ['－', () => { zoomRef.current = Math.max(0.1, zoomRef.current / 1.3); redraw(); }],
            ['⌂',  resetView]
          ].map(([lbl, fn]) => (
            <button key={lbl} onClick={fn} style={{
              width: 28, height: 28, background: '#13151c', border: '1px solid #1e2030',
              borderRadius: 4, color: '#89b4fa', cursor: 'pointer', fontSize: 14,
              display: 'flex', alignItems: 'center', justifyContent: 'center',
            }}>{lbl}</button>
          ))}
        </div>

        {/* Hint */}
        <div style={{ position: 'absolute', top: 10, left: 10, fontSize: 10, color: '#313244', pointerEvents: 'none' }}>
          Scroll to zoom · Alt+drag to pan · Click to inspect
        </div>

        {/* Status bar (bottom) */}
        <div style={{
          position: 'absolute', bottom: 0, left: 0, right: 0,
          background: '#13151ccc', borderTop: '1px solid #1e2030',
          padding: '3px 12px', display: 'flex', gap: 20, fontSize: 10, color: '#585b70',
          backdropFilter: 'blur(4px)',
        }}>
          <span>x: {cursor.wx.toFixed(4)}</span>
          <span>y: {cursor.wy.toFixed(4)}</span>
          <span style={{ marginLeft: 'auto' }}>zoom: {(zoomRef.current * 100).toFixed(0)}%</span>
        </div>
      </div>

      {/* ── INSPECTOR PANEL (right) ── */}
      <div style={{ width: 200, background: '#13151c', borderLeft: '1px solid #1e2030', display: 'flex', flexDirection: 'column', flexShrink: 0 }}>
        <div style={{ padding: '10px 12px 6px', borderBottom: '1px solid #1e2030' }}>
          <span style={{ fontSize: 11, fontWeight: 700, letterSpacing: 1, color: '#89b4fa', textTransform: 'uppercase' }}>Inspector</span>
        </div>
        <div style={{ flex: 1, padding: '8px 12px', overflowY: 'auto' }}>
          {(selected || (hovered !== -1 && data)) ? (() => {
            const m = selected || data.mbrs[hovered];
            const { stroke } = color(m.level);
            return (
              <div>
                <div style={{ color: stroke, fontWeight: 700, marginBottom: 8, fontSize: 11 }}>
                  {m.is_leaf ? '◆ Leaf' : '◇ Internal'} · Level {m.level}
                </div>
                {[
                  ['page_id',   m.page_id],
                  ['n_entries', m.n_entries],
                  ['min_x',     m.min_x?.toFixed(5)],
                  ['min_y',     m.min_y?.toFixed(5)],
                  ['max_x',     m.max_x?.toFixed(5)],
                  ['max_y',     m.max_y?.toFixed(5)],
                  ['width',     (m.max_x - m.min_x)?.toFixed(5)],
                  ['height',    (m.max_y - m.min_y)?.toFixed(5)],
                  ['area',      ((m.max_x - m.min_x) * (m.max_y - m.min_y))?.toFixed(5)],
                ].map(([k, v]) => (
                  <div key={k} style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 5, gap: 4 }}>
                    <span style={{ color: '#585b70', fontSize: 10 }}>{k}</span>
                    <span style={{ color: '#cdd6f4', fontSize: 10, fontWeight: 600 }}>{v}</span>
                  </div>
                ))}
              </div>
            );
          })() : (
            <p style={{ color: '#313244', fontSize: 10, marginTop: 4 }}>Hover or click a node to inspect it.</p>
          )}
        </div>
      </div>

      {/* ── TOOLTIP (floating) ── */}
      {tooltip && !selected && (
        <div style={{
          position: 'fixed', left: tooltip.x, top: tooltip.y, zIndex: 9999,
          background: '#181825', border: '1px solid #313244', borderRadius: 6,
          padding: '8px 12px', pointerEvents: 'none', boxShadow: '0 8px 24px #00000088',
          fontSize: 11,
        }}>
          <div style={{ color: color(tooltip.mbr.level).stroke, fontWeight: 700, marginBottom: 4 }}>
            {tooltip.mbr.is_leaf ? 'Leaf' : 'Internal'} — Level {tooltip.mbr.level}
          </div>
          <div style={{ color: '#7f849c' }}>
            [{tooltip.mbr.min_x?.toFixed(3)}, {tooltip.mbr.min_y?.toFixed(3)}] →
            [{tooltip.mbr.max_x?.toFixed(3)}, {tooltip.mbr.max_y?.toFixed(3)}]
          </div>
          <div style={{ color: '#7f849c', marginTop: 2 }}>
            {tooltip.mbr.n_entries} entries · page #{tooltip.mbr.page_id}
          </div>
        </div>
      )}
    </div>
  );
}

// ── micro-components ──────────────────────────────────
function QBtn({ children, onClick, style = {} }) {
  return (
    <button onClick={onClick} style={{
      flex: 1, padding: '4px 0', fontSize: 10, background: '#1e2030',
      border: '1px solid #313244', borderRadius: 4, color: '#89b4fa',
      cursor: 'pointer', ...style,
    }}>{children}</button>
  );
}
function StatRow({ k, v }) {
  return (
    <div style={{ display: 'flex', justifyContent: 'space-between' }}>
      <span style={{ color: '#45475a', fontSize: 10 }}>{k}</span>
      <span style={{ color: '#bac2de', fontSize: 10, fontWeight: 600 }}>{v}</span>
    </div>
  );
}
function Overlay({ children, color = '#585b70' }) {
  return (
    <div style={{
      position: 'absolute', inset: 0, display: 'flex', alignItems: 'center',
      justifyContent: 'center', gap: 8, color, fontSize: 13, pointerEvents: 'none',
    }}>{children}</div>
  );
}
function Spin() {
  return (
    <svg width={14} height={14} viewBox='0 0 24 24' fill='none' style={{ animation: 'spin 0.8s linear infinite' }}>
      <circle cx={12} cy={12} r={10} stroke='#313244' strokeWidth={2} />
      <path d='M12 2a10 10 0 0 1 10 10' stroke='#89b4fa' strokeWidth={2} strokeLinecap='round'>
        <animateTransform attributeName='transform' type='rotate' from='0 12 12' to='360 12 12' dur='0.8s' repeatCount='indefinite' />
      </path>
    </svg>
  );
}

// ─────────────────────────────────────────────────────────────────────────────
// BOTTOM TABS + ORIGINAL APP (unchanged except new "Spatial" tab)
// ─────────────────────────────────────────────────────────────────────────────

const BOTTOM_TABS = [
  { id: 'results',       label: 'Data Output'   },
  { id: 'spatial',       label: '🗺  Spatial'    },
  { id: 'messages',      label: 'Messages'       },
  { id: 'notifications', label: 'Notifications'  },
];

export default function App() {
  const [tables, setTables]             = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableInfo, setTableInfo]       = useState(null);
  const [results, setResults]           = useState(null);
  const [stats, setStats]               = useState(null);
  const [error, setError]               = useState(null);
  const [successMsg, setSuccessMsg]     = useState(null);
  const [notifications, setNotifications] = useState([]);
  const [loading, setLoading]           = useState(false);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [activeTab, setActiveTab]       = useState('results');
  const [cursor, setCursor]             = useState({ line: 1, col: 1 });
  const [maximized, setMaximized]       = useState(null);

  const [editorHeight, setEditorHeight] = useState(220);
  const dragging = useRef(false);
  const containerRef = useRef(null);

  const onMouseDown = (e) => {
    e.preventDefault();
    dragging.current = true;
    document.body.style.cursor = 'row-resize';
    document.body.style.userSelect = 'none';
  };

  useEffect(() => {
    const onMouseMove = (e) => {
      if (!dragging.current || !containerRef.current) return;
      const rect = containerRef.current.getBoundingClientRect();
      setEditorHeight(Math.max(80, Math.min(e.clientY - rect.top, rect.height - 120)));
    };
    const onMouseUp = () => {
      dragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    };
    window.addEventListener('mousemove', onMouseMove);
    window.addEventListener('mouseup', onMouseUp);
    return () => { window.removeEventListener('mousemove', onMouseMove); window.removeEventListener('mouseup', onMouseUp); };
  }, []);

  const refreshTables = useCallback(async () => {
    try { const d = await getTables(); setTables(d.tables || []); } catch { setTables([]); }
  }, []);

  useEffect(() => { refreshTables(); }, [refreshTables]);

  const handleSelectTable = async (name) => {
    setSelectedTable(name);
    try { setTableInfo(await getTable(name)); } catch { setTableInfo(null); }
  };

  const handleDropTable = async (name) => {
    try {
      await dropTable(name);
      if (selectedTable === name) { setSelectedTable(null); setTableInfo(null); }
      pushNotification('success', `Table "${name}" dropped.`);
      refreshTables();
    } catch (e) { setError(e.message); pushNotification('error', e.message); }
  };

  const pushNotification = (kind, text) =>
    setNotifications(prev => [{ kind, text, time: new Date().toLocaleTimeString() }, ...prev].slice(0, 50));

  const handleExecute = async (sqlText, columnSizes) => {
    setError(null); setSuccessMsg(null); setLoading(true);
    try {
      const data = await runQuery(sqlText, columnSizes);
      if (data.rows?.length > 0) {
        setResults(data.rows); setStats(data.stats || null);
        setSuccessMsg(null); setActiveTab('results');
      } else {
        setResults(null); setStats(data.stats || null);
        const up = sqlText.trim().toUpperCase();
        const msg = up.startsWith('INSERT') ? 'Row inserted successfully.'
          : up.startsWith('DELETE') ? 'Delete executed successfully.'
          : up.startsWith('CREATE') ? `Table "${data.table}" created successfully.`
          : 'Query executed. 0 rows returned.';
        setSuccessMsg(msg);
        pushNotification('success', msg);
        setActiveTab('messages');
        if (data.table) refreshTables();
      }
    } catch (e) {
      setError(e.message); setResults(null); setStats(null); setSuccessMsg(null);
      pushNotification('error', e.message); setActiveTab('messages');
    } finally { setLoading(false); }
  };

  const toggleMaximize = (which) => setMaximized(prev => prev === which ? null : which);
  const showEditor = maximized !== 'bottom';
  const showBottom = maximized !== 'editor';
  const totalRows  = results?.length ?? 0;

  // For spatial tab: find the last RTREE table selected, or any rtree table
  const spatialTable = selectedTable || null;

  return (
    <div className='flex flex-col h-screen overflow-hidden bg-bg-primary'>
      <div className='flex flex-1 min-h-0 overflow-hidden' ref={containerRef}>
        <Sidebar
          tables={tables} selectedTable={selectedTable} tableInfo={tableInfo}
          onSelectTable={handleSelectTable} onDropTable={handleDropTable}
          onCreateTable={() => setShowCreateModal(true)} onRefresh={refreshTables}
        />

        <div className='flex flex-col flex-1 min-w-0'>
          {showEditor && (
            <div style={{ height: maximized === 'editor' ? '100%' : editorHeight, minHeight: 80, flexShrink: 0 }}>
              <QueryEditor
                onExecute={handleExecute} loading={loading}
                onCursorChange={setCursor}
                onToggleMaximize={() => toggleMaximize('editor')}
                isMaximized={maximized === 'editor'}
              />
            </div>
          )}

          {showEditor && showBottom && (
            <div onMouseDown={onMouseDown}
              className='h-1.5 bg-bg-secondary border-y border-border cursor-row-resize hover:bg-accent/20 active:bg-accent/30 transition-colors flex items-center justify-center shrink-0'>
              <div className='w-10 h-0.5 rounded bg-border' />
            </div>
          )}

          {showBottom && (
            <div className='flex flex-col flex-1 min-h-0'>
              <div className='flex items-stretch bg-bg-primary border-b border-border h-9 shrink-0'>
                {BOTTOM_TABS.map(tab => (
                  <button key={tab.id} onClick={() => setActiveTab(tab.id)}
                    className={`relative px-4 text-[13px] transition-colors ${activeTab === tab.id ? 'text-text-primary' : 'text-text-secondary hover:text-text-primary'}`}>
                    {tab.label}
                    {activeTab === tab.id && <span className='absolute bottom-0 left-4 right-4 h-px bg-text-primary' />}
                  </button>
                ))}
                <div className='ml-auto flex items-center pr-2'>
                  <button onClick={() => toggleMaximize('bottom')}
                    className='p-1.5 text-text-secondary hover:text-text-primary transition-colors'
                    title={maximized === 'bottom' ? 'Restore' : 'Maximize'}>
                    {maximized === 'bottom' ? <Minimize2 size={14} /> : <Maximize2 size={14} />}
                  </button>
                </div>
              </div>

              <div className='flex-1 overflow-auto'>
                {activeTab === 'results'       && <ResultsPanel results={results} loading={loading} />}
                {activeTab === 'spatial'       && <RTreeViewer tableName={spatialTable} />}
                {activeTab === 'messages'      && <MessagesPanel error={error} successMsg={successMsg} stats={stats} />}
                {activeTab === 'notifications' && <NotificationsPanel notifications={notifications} />}
              </div>
            </div>
          )}
        </div>
      </div>

      <div className='flex items-center bg-bg-secondary border-t border-border px-3 text-[11px] text-text-muted shrink-0 h-6 select-none'>
        <span>Total rows:{results ? ` ${totalRows}` : ''}</span>
        <div className='ml-auto flex items-center gap-4'>
          <span>LF</span>
          <span>Ln {cursor.line}, Col {cursor.col}</span>
        </div>
      </div>

      {showCreateModal && (
        <CreateTableModal
          onClose={() => setShowCreateModal(false)}
          onExecute={(sql, sizes) => { handleExecute(sql, sizes); setShowCreateModal(false); }}
        />
      )}
    </div>
  );
}

function MessagesPanel({ error, successMsg, stats }) {
  if (!error && !successMsg && !stats) return <p className='p-3 text-xs text-text-muted'>No messages.</p>;
  return (
    <div className='p-3 space-y-2 animate-fade-in'>
      {error && (
        <div className='flex items-start gap-2 bg-error-subtle border border-error/20 rounded p-2.5'>
          <AlertCircle size={14} className='text-error mt-0.5 shrink-0' />
          <span className='text-xs text-error font-mono break-all'>{error}</span>
        </div>
      )}
      {successMsg && (
        <div className='flex items-start gap-2 bg-success-subtle border border-success/20 rounded p-2.5'>
          <CheckCircle2 size={14} className='text-success mt-0.5 shrink-0' />
          <span className='text-xs text-success'>{successMsg}</span>
        </div>
      )}
      {stats && <div className='bg-bg-secondary border border-border/40 rounded p-2.5'><StatsBar stats={stats} /></div>}
    </div>
  );
}

function NotificationsPanel({ notifications }) {
  if (!notifications.length) return (
    <div className='flex flex-col items-center justify-center h-full py-12 text-text-muted'>
      <Info size={24} className='mb-2 opacity-30' />
      <p className='text-xs'>No notifications.</p>
    </div>
  );
  return (
    <div className='animate-fade-in'>
      {notifications.map((n, i) => (
        <div key={i} className='flex items-start gap-2 px-3 py-2 border-b border-border/40'>
          {n.kind === 'error'
            ? <AlertCircle size={13} className='text-error mt-0.5 shrink-0' />
            : <CheckCircle2 size={13} className='text-success mt-0.5 shrink-0' />}
          <span className='text-xs text-text-muted shrink-0 font-mono'>{n.time}</span>
          <span className='text-xs text-text-primary break-all'>{n.text}</span>
        </div>
      ))}
    </div>
  );
}