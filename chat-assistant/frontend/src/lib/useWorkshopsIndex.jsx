import {useEffect, useMemo, useState} from "react";

const BASE = "/content/workshops";

export default function useWorkshopsIndex() {
  const [list, setList] = useState(null);
  const [error, setError] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try{
        const res = await fetch (`${BASE}/workshopIndex.json`, {cache : "no-store"});
        if(!res.ok) throw new Error(`Index fetch failed: ${res.status}`);
        const data = await res.json();
        if(!cancelled) setList(data);
      } catch (e) {
        if(!cancelled) setError(e);
      } finally {
        if(!cancelled) setLoading(false);
      }
    })();
    return () => {cancelled = true;};
  }, []);

  const bySlug = useMemo(() => {
    if(!list) return () => undefined;
    const map = new Map(list.map(w =>[w.slug, w]));
    return (slug)  => map.get(slug);
  }, [list]);

  const fileUrlFor = (file) => `${BASE}/${file}`;

  return {list, loading, error, bySlug, fileUrlFor};
}