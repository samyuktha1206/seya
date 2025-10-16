import Slide from "./Slide";
import Questionnaire from "./Questionnaire";

export default function ContentRenderer({item}){
  if(!item) return <div className = "text-gray-500">No Content.</div>

  switch(item.type) {
    case "slide":
      return <Slide slide = {item}/>
    
    case "questionnaire":
      return <Questionnaire block = {item}/>

    default:
      return (
        <div className = "text-red-500">
          Unsupported item type:<b>{String(item.type || "unknown")}</b>
        </div>
      );
  }
}
