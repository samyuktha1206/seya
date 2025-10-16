import {useEffect, useState} from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

function Table({table}) {
  if(!table?.columns || !table?.rows) return null

  return (
    <div className = "overflow-x-auto rounded border">
      <table className = "min-w-full text-sm">
        <thead className = "bg-gray-50">
          <tr>
            {table.columns.map((c,i) => (
              <th key = {i} className = "px-3 py-2 text-left front-medium text-gray-700">
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {table.rows.map((r, ri) => (
            <tr key = {ri} className = "border-t">
              {row.map((cell, ci) => (
                <td key = {ci} className = "px-3 py-2">
                  {typeof cell === "string" && cell.startsWith("http")?(
                    <a
                      href = {cell}
                      target = "_blank"
                      rel = "noopener noreferrer"
                      className = "text-blue-600 underline"
                    >
                      open
                    </a>
                  ) : (cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default function Slide({slide}) {
  const [journals, setJournals] = useState(
    slide.journalQuestions ? slide.journalQuestions.map(() => "") : []
  );

  useEffect(() => {
    setJournals(slide.journalQuestions? slide.journalQuestions.map(() => ""): []);
  }, [slide.id]);

  const setJ = (idx, val) => setJournals(prev => prev.map((v,i) => (i===idx)?val:v));

  return (
    <div>
      {/* {slide.title && <h3 className = "text-xl font-semibold">{slide.title}</h3>} */}
      {slide.content && (
        <ReactMarkdown
          remarkPlugins = {[remarkGfm]}
          components = {{
            a: (props) => (
              <a
                {...props}
                target = "_blank"
                rel = "noopener noreferrer"
                className = "text-blue-600 underline"
              />
            )
          }}
        >
          {slide.content}
        </ReactMarkdown>
      )}

      {slide.journalQuestions?.length > 0 && (
        <div className = "space-y-4">
          {slide.journalQuestions.map((q,i) => (
            <div key = {i} className = "space-y-2">
              <p className = "font-medium">{q}</p>
              <textarea
                className = "w-full borded rounded p-2"
                rows = {3}
                placeholder = "Write your thoughts here (optional)..."
                value = {journals[i] || ""}
                onChange = {(e) => setJ(i, e.target.value)}
              />
            </div>
          ))}
        </div>
      )}
      {slide.resources?.length > 0 && (
        <div className = "space-y-2">
          <h4 className = "font-semibold">Resources</h4>
          <ul className = "list-disc pl-5 space-y-1">
            {slide.resources.map((r,i) => (
              <li key = {i}>
                <a
                  href = {r.url}
                  target = "_blank"
                  rel = "noopener noreferrer"
                  className = "text-blue-600 underline"
                >
                  {r.label}
                </a>
              </li>
            ))}
          </ul>
        </div>
      )}
      {slide.table && <Table table = {slide.table}/>}
    </div>
  );
}