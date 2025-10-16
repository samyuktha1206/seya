// src/pages/Workshops.jsx
import { Link } from "react-router-dom";
import useWorkshopsIndex from "../lib/useWorkshopsIndex.jsx";

export default function Workshops() {
  const { list, loading, error } = useWorkshopsIndex();

  if (loading) return <p className="py-10 text-center">Loading…</p>;
  if (error) {
    console.error(error);
    return <p className="py-10 text-center text-red-600">Failed to load workshops.</p>;
  }

  const items = Array.isArray(list) ? list : [];

  return (
    <main className="min-h-screen bg-gray-50">
      <div className="mx-auto max-w-4xl px-4 py-8">
        {/* Page heading */}
        <h1 className="text-10xl font-bold text-center mb-10">Workshops</h1>

        <div className="space-y-6">
          {items.map((w) => (
            <Link
              key={w.id}
              to={`/workshops/${encodeURIComponent(w.slug)}/intro`}
              className="
                flex gap-6 items-center
                rounded-xl border bg-white p-4 shadow-sm
                hover:shadow-md transition-shadow
              "
            >
              {/* Thumbnail */}
              <div className="w-40 aspect-video rounded-lg bg-gradient-to-br from-blue-100 to-blue-300 flex items-center justify-center text-blue-700 font-bold">
                {w.icon || "ICON"}
              </div>

              {/* Text */}
              <div className="flex-1">
                <h3 className="text-lg font-semibold text-gray-900">{w.title}</h3>
                <p className="text-sm text-gray-600 mt-1">{w.snippet}</p>
                <div className="text-xs text-gray-500 mt-2">
                  Duration: {w.durationMins} mins
                </div>
              </div>
            </Link>
          ))}
        </div>
      </div>
    </main>
  );
}
