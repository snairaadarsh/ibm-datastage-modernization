const BASE = '/api'

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, options)
  if (!res.ok) {
    const err = await res.json().catch(() => ({ error: res.statusText }))
    throw new Error(err.error || 'Request failed')
  }
  return res.json()
}

export const api = {
  // Upload a real file with provider configuration
  uploadFile: (file, providerConfig = {}) => {
    const form = new FormData()
    form.append('file', file)
    form.append('provider', providerConfig.provider || 'gemini')
    form.append('apiKey',   providerConfig.apiKey   || '')
    form.append('model',    providerConfig.model    || '')
    return request('/upload', { method: 'POST', body: form })
  },

  // Jobs
  getJobs: () => request('/jobs'),
  getJob:  (id) => request(`/jobs/${id}`),
  deleteJob: (id) => request(`/jobs/${id}`, { method: 'DELETE' }),

  // Human review actions
  approveJob: (id) => request(`/jobs/${id}/approve`, { method: 'POST' }),
  rejectJob: (id, reason) =>
    request(`/jobs/${id}/reject`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason }),
    }),
  requestChanges: (id, notes) =>
    request(`/jobs/${id}/request-changes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ notes }),
    }),
  // New unified decision: proceed | stop | comment
  submitHumanReviewDecision: (id, decision, comment = '') =>
    request(`/jobs/${id}/human-review-decision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision, comment }),
    }),

  // Validation review decision: proceed | ai_fix | comment
  submitValidationDecision: (id, decision, comment = '') =>
    request(`/jobs/${id}/validation-decision`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ decision, comment }),
    }),


  // AG Chat bridge
  submitAgChatResponse: (requestId, response) =>
    request(`/ag-chat/response/${requestId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ response }),
    }),
  cancelAgChatPrompt: (requestId) =>
    request(`/ag-chat/prompt/${requestId}`, { method: 'DELETE' }),

  // Download helpers
  downloadUrl: (jobId, type) => `${BASE}/download/${jobId}/${type}`,
  downloadFile: (jobId, type) => {
    const a = document.createElement('a')
    a.href = `${BASE}/download/${jobId}/${type}`
    a.download = true
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
  },
}
