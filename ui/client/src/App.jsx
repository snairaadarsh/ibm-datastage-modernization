import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import { AnimatePresence } from 'framer-motion'
import Nav           from './components/Nav.jsx'
import AgChatBridge  from './components/AgChatBridge.jsx'
import Landing          from './pages/Landing.jsx'
import Upload           from './pages/Upload.jsx'
import Pipeline         from './pages/Pipeline.jsx'
import HumanReview      from './pages/HumanReview.jsx'
import ValidationReview from './pages/ValidationReview.jsx'
import Results          from './pages/Results.jsx'
import Dashboard        from './pages/Dashboard.jsx'

export default function App() {
  return (
    <BrowserRouter>
      <div className="app-shell">
        <Nav />
        <main className="app-main">
          <AnimatePresence mode="wait">
            <Routes>
              <Route path="/"                      element={<Landing />} />
              <Route path="/upload"               element={<Upload />} />
              <Route path="/pipeline/:jobId"      element={<Pipeline />} />
              <Route path="/review/:jobId"        element={<HumanReview />} />
              <Route path="/validation/:jobId"    element={<ValidationReview />} />
              <Route path="/results/:jobId"       element={<Results />} />
              <Route path="/dashboard"            element={<Dashboard />} />
              <Route path="*"                     element={<Navigate to="/" replace />} />
            </Routes>
          </AnimatePresence>
        </main>
        {/* AG Chat bridge — globally mounted, only renders when active */}
        <AgChatBridge />
      </div>
    </BrowserRouter>
  )
}
