import { BrowserRouter, Routes, Route, Navigate} from "react-router-dom";
import Workshops from "./pages/Workshops.jsx";
import WorkshopLanding from "./pages/WorkshopLanding.jsx";
import Workshop from "./pages/Workshop.jsx";
import w01 from "../public/content/workshops/w01.json";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path = "/" element = {<Workshops/>}/>
        <Route path = "/workshops" element = {<Workshops/>}/>
        <Route path = "/workshops/:slug/intro" element = {<WorkshopLanding />}/>
        <Route path = "/workshops/:slug" element = {<Workshop/>}/>
        <Route path = "*" element = {<Navigate to = "/" replace />}/>
      </Routes>
    </BrowserRouter>
  );
}