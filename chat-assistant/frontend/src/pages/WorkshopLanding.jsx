import { useNavigate, useParams } from "react-router-dom";
import useWorkshopsIndex from "../lib/useWorkshopsIndex.jsx";

export default function WorkshopLanding() {
  const navigate = useNavigate();
  const {slug} = useParams();
  const {bySlug, loading, error} = useWorkshopsIndex();

  if (loading) return <p style={{ padding: 24 }}>Loading…</p>;
  if (error) return <p style={{ padding: 24, color: "crimson" }}>Failed to load index.</p>;

  const workshop = bySlug(slug);

  const start = () => navigate(`/workshops/${encodeURIComponent(workshop.slug)}`);

  return (
    <div className = "min-h-screen bg-white text-gray-800">
      <div className = "max-w-3xl mx-auto p-6 space-y-6">
        <h1 className = "text-3xl font-bold">{workshop.title}</h1>
        <p className = "text-sm text-gray-500">Duration: {workshop.durationMins} mins</p>
        {workshop.intro && (<pre className = "whitespace-pre-wrap bg-gray-50 p-4 rounded-border">{workshop.intro}</pre>
        )}
        <button 
        onClick = {start}
        className = "px-4 py-2 rounded bg-blue-600 text-white hover:bg-blue-700"
        > Start Learning </button>
      </div>
    </div>
  );
}