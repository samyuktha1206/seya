import {useState} from "react";

export default function Questionnaire({block}) {
  const [answers, setAnswers] = useState({});
  const setAns = (idx, value) => setAnswers(prev => ({...prev, [idx] : value}));

  return (
    <div className = "space-y-5">
      {block.titile && <h3 className = "text-xl font-semibold">{block.title}</h3>}
      {block.content && <pre className = "whitespace-pre-wrap bg-gray-50 p-3 rounded border">{block.content}</pre>}

      {(block.questions || []).map((q,i) =>{
        //free text
        if(q.kind === "text"){
          return (
            <div key={i} className = "space-y-2">
              <p className = "font-medium">{q.question}</p>
              <input
                className = "w-full border rounded p-2"
                placeholder = {q.placeholder || "Type your answer"}
                value = {answers[i] || ""}
                onChange = {(e) => setAns(i, e.target.value)}
              />
            </div>
          );
        }
        //single choice with optional follow-up

        const options = (q.options || []).map(opt =>
          typeof opt === "string"? {value : opt} : opt
        );
        return (
          <div key = {i} className = "space-y-2">
            <p className = "font-medium">{q.question}</p>
            <div className = "flex flex-col gap-2">
              {options.map((opt, oi) => {
                const selected = answers[i]?.value === opt.value;
                const follow = opt["follow-up"];
                return (
                  <div key = {oi} className = "flex flex-col gap-2">
                    <label className = "inline-flex items-center gap-2">
                      <input
                        type = "radio"
                        name = {`q-${i}`}
                        checked = {selected}
                        onChange = {() => setAns(i, {value : opt.value, follow: follow? "" : undefined})}
                      />
                      <span>{opt.value}</span>
                    </label>
                    {selected && follow && (
                      <input
                        className = "ml-6 border rounded p-2"
                        placeholder = {follow.placeholder|| "Type your answer."} 
                        required = {String(follow.required).toLowerCase === "true"}
                        value = {answers[i]?.follow || ""}
                        onChange = {(e) =>
                          setAns(i, {value:opt.value, follow: e.target.value})
                        }
                      />
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
}