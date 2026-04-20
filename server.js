const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 8080;

app.use(cors());
app.use(express.json({ limit: '10mb' }));

const API_BASE = 'https://integrate.api.nvidia.com/v1';
const API_KEY = process.env.NIM_API_KEY;

app.all('*', (req, res, next) => {
  if (req.method !== 'POST') {
    return res.json({ status: 'ok' });
  }
  next();
});

app.post('*', async (req, res) => {
  try {
    const { model, stream } = req.body;
    console.log(`[REQ] model=${model} stream=${stream}`);

    const response = await axios.post(`${API_BASE}/chat/completions`, req.body, {
      headers: {
        'Authorization': `Bearer ${API_KEY}`,
        'Content-Type': 'application/json'
      },
      responseType: stream ? 'stream' : 'json',
      timeout: 60000
    });

    console.log(`[OK] ${response.status}`);

    if (stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      let buffer = '';
      response.data.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        lines.forEach(line => {
          if (!line.startsWith('data: ')) return;
          if (line.includes('[DONE]')) { res.write(line + '\n'); return; }
          try {
            const data = JSON.parse(line.slice(6));
            if (data.choices?.[0]?.delta) {
              data.choices[0].delta.content = data.choices[0].delta.content || '';
              delete data.choices[0].delta.reasoning_content;
            }
            res.write(`data: ${JSON.stringify(data)}\n\n`);
          } catch (e) { res.write(line + '\n'); }
        });
      });
      response.data.on('end', () => res.end());
      response.data.on('error', () => res.end());

    } else {
      res.json({
        id: `chatcmpl-${Date.now()}`,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model,
        choices: response.data.choices.map(c => ({
          index: c.index,
          message: { role: c.message.role, content: c.message.content || '' },
          finish_reason: c.finish_reason
        })),
        usage: response.data.usage || {}
      });
    }

  } catch (err) {
    console.error('[ERR]', err.message, JSON.stringify(err.response?.data));
    res.status(err.response?.status || 500).json({
      error: { message: err.message, code: err.response?.status || 500 }
    });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
  console.log(`API_KEY set: ${!!API_KEY}`);
});
