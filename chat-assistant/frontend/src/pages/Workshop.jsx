// Workshop.jsx
import { useMemo, useState, useEffect } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import Sidebar from "../components/Sidebar";
import ContentRenderer from "../components/ContentRenderer";
import useWorkshopsIndex from "../lib/useWorkshopsIndex.jsx";
import TopBar from "../components/TopBar.jsx";

function buildFlatIndex(modules) {
  const out = [];
  modules.forEach((m, mi) => (m.items || []).forEach((_, ii) => out.push({ mi, ii })));
  return out;
}

export default function Workshop() {
  const { slug } = useParams();
  const [searchParams] = useSearchParams();
  const fileFromQuery = searchParams.get("file");

  const { bySlug, fileUrlFor, loading, error } = useWorkshopsIndex();

  const [workshop, setWorkshop] = useState(null);
  const [err, setErr] = useState(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        if (loading || error) return;
        let file = fileFromQuery;
        if (!file) {
          const entry = bySlug(slug);
          if (!entry) throw new Error("Workshop not found in index");
          file = entry.file;
        }
        const url = file.startsWith("http") ? file : fileUrlFor(file);
        const res = await fetch(url, { cache: "no-store" });
        if (!res.ok) throw new Error(`Workshop fetch failed: ${res.status}`);
        const data = await res.json();
        if (!cancelled) setWorkshop(data);
      } catch (e) {
        if (!cancelled) setErr(e);
      }
    }
    load();
    return () => { cancelled = true; };
  }, [slug, fileFromQuery, bySlug, fileUrlFor, loading, error]);

  const modules = workshop?.modules || [];
  const flat = useMemo(() => buildFlatIndex(modules), [modules]);
  const [pos, setPos] = useState(flat.length ? 0 : -1);

  useEffect(() => {
    if (flat.length && (pos < 0 || pos >= flat.length)) setPos(0);
  }, [flat.length]);

  if (loading) return <p style={{ padding: 24 }}>Loading…</p>;
  if (error) return <p style={{ padding: 24, color: "crimson" }}>Failed to load index.</p>;
  if (err) return <p style={{ padding: 24, color: "crimson" }}>{String(err)}</p>;
  if (!workshop) return <p style={{ padding: 24 }}>Loading workshop…</p>;

  const mi = pos >= 0 ? flat[pos].mi : 0;
  const ii = pos >= 0 ? flat[pos].ii : 0;

  const currentModule = modules[mi] || { items: [] };
  const currentItem = (currentModule.items || [])[ii];

  const goPrev = () => setPos((p) => (p > 0 ? p - 1 : p));
  const goNext = () => setPos((p) => (p >= 0 && p < flat.length - 1 ? p + 1 : p));
  const jumpTo = (newMi, newIi) => {
    const idx = flat.findIndex((e) => e.mi === newMi && e.ii === newIi);
    if (idx !== -1) setPos(idx);
  };

  const hasPrev = pos > 0;
  const hasNext = pos >= 0 && pos < flat.length - 1;

  // NEW: compute background styles from currentItem.bg
  const bg = currentItem?.bg; // CHANGED
  const bgStyle = bg
    ? {
        backgroundImage: `url(${bg.url})`,
        backgroundSize: bg.fit === "contain" ? "contain" : "cover",
        backgroundPosition: bg.position || "center",
        backgroundRepeat: "no-repeat",
      }
    : undefined; // CHANGED
  const overlayOpacity = typeof bg?.overlay === "number" ? bg.overlay : 0; // CHANGED

  return (
    <div className="h-screen flex flex-col">
      <TopBar />

      <div className="flex-1 grid grid-cols-[400px_minmax(0,1fr)] min-h-0">
        {/* Sidebar */}
        <aside className=" bg-gray-50 overflow-y-auto"> {/* CHANGED: border-r */}
          <Sidebar
            workshopttl={workshop.title}
            modules={modules}
            activeModuleIndex={mi}
            activeItemIndex={ii}
            onSelect={jumpTo}
          />
        </aside>

        {/* Right pane */}
        {/* Right pane */}
<main
  className="relative flex flex-col h-full min-h-0 p-6 space-y-4 overflow-hidden"
>
  {/* BG layer fills the whole right container */}
  {bg && <div className="absolute inset-0" style={bgStyle} aria-hidden="true" />}
  {bg && (
    <div
      className="absolute inset-0"
      style={{ opacity: overlayOpacity }}
      aria-hidden="true"
    />
  )}

  {/* Foreground content stays above background */}
  <div className="relative z-10 flex flex-col h-full">
    <header className="space-y-1 shrink-0">
      <h1 className="text-lg text-pink-700 font-semibold">
        {currentItem?.title}
      </h1>
    </header>

    {/* CONTENT AREA WITH SCROLL */}
    <section className="flex-1 overflow-y-auto rounded p-4 bg-white/20">
      <ContentRenderer item={currentItem} />
    </section>

    <div className="flex items-center justify-between pt-4 shrink-0">
      <button
        onClick={goPrev}
        className="px-4 py-2 rounded border hover:bg-gray-100 disabled:opacity-50"
        disabled={!hasPrev}
      >
        Prev
      </button>
      <button
        onClick={goNext}
        className="px-4 py-2 rounded bg-yellow-500 text-white hover:bg-pink-500 disabled:opacity-40"
        disabled={!hasNext}
      >
        Next
      </button>
    </div>
  </div>
</main>

      </div>
    </div>
  );
}
