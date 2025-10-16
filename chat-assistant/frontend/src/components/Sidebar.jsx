import {useState} from "react";

export default function Sidebar({
  workshopttl = "",
  modules = [],
  activeModuleIndex = 0,
  activeItemIndex = 0,
  onSelect
}) {

  const safeOnSelect = typeof onSelect === "function" ? onSelect : () => {};

  const [open, setOpen] = useState(() => modules.map((_,i) => i === 0));

  const toggle = (i) =>
    setOpen(prev => prev.map((v,idx) =>(idx === i? !v : v)));

  return (
    <div className = "p-4 space-y-5">
      <h1 className = "text-2xl font-semibold text-yellow-600">{workshopttl}</h1>
      <div className = "space-y-2">
        {modules.map((m, mi) =>{
          const isOpen = open[mi];
          const items = m.items|| [];
          return (
            <div key = {m.id || mi} className = "border-2 border-yellow-600 rounded bg-white">
              <button
                className = "w-full text-left px-3 py-2 flex items-center justify-between"
                onClick = {() => toggle(mi)}
              >
                <span className = "font-medium text-yellow-600">{m.title}</span>
                <span className = "text-pink-800">{isOpen ? "-" : "+"}</span> 
              </button>
              {isOpen && items.length > 0 && (
                <ul className = "border-t">
                  {items.map((it, ii) =>{
                    const active = mi == activeModuleIndex && ii == activeItemIndex;
                    const label = it.title || (it.type == "questionnaire" ? "questionnaire" : "slide");
                    return (
                      <li key = {it.id || ii}>
                        <button 
                          onClick = {() => safeOnSelect(mi, ii)}
                          className = {["w-full text-left px-4 py-2", "hover: bg-gray-50", active?"bg-blue-50":"",].join(" ")}
                          title={it.title || it.type}
                        >
                          {/* <span className = "text-[11px] uppercase text-gray-500 mr-2">
                            {it.type}
                          </span> */}
                          <span>{label}</span>
                        </button>
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          );
        })}
      </div>
    </div>
  )
}