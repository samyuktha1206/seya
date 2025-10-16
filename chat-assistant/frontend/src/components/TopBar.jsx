// NEW: simple top bar with right-aligned links (placeholders)
export default function TopBar() {
  const stop = (e) => e.preventDefault(); // prevent navigation for now
  return (
    <header className="h-14 shrink-0 bg-yellow-500">
      <div className="h-full max-w-10xl mx-auto px-4 flex items-center justify-end gap-10">
        <a href="#" onClick={stop} className="text-lg font-semibold text-white">
          Home
        </a>
        <a href="#" onClick={stop} className="text-lg font-semibold text-white">
          Workshops
        </a>
      </div>
    </header>
  );
}
