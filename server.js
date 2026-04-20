const express = require('express');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const NIM_API_BASE = 'https://integrate.api.nvidia.com/v1';
const NIM_API_KEY = process.env.NIM_API_KEY;

if (!NIM_API_KEY) {
  console.error('FATAL: NIM_API_KEY is missing.');
  process.exit(1);
}

// Health Check for Railway
app.all('/', (req, res) => res.json({ status: 'ok', service: 'Smarter NIM Proxy' }));

app.post(['/v1/chat/completions', '/chat/completions'], async (req, res) => {
  try {
    const body = req.body;
    console.log(`[REQUEST] model=${body.model} stream=${!!body.stream}`);

    // Native Node Fetch (Faster, no Axios dependency)
    const response = await fetch(`${NIM_API_BASE}/chat/completions`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(body)
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      return res.status(response.status).json(errorData);
    }

    if (body.stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');

      const reader = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      
      let isThinking = false;
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; // Keep the incomplete chunk for the next loop

        for (const line of lines) {
          if (line.trim() === '') continue;
          if (line.startsWith('data: ')) {
            if (line.includes('[DONE]')) {
              if (isThinking) {
                res.write(`data: ${JSON.stringify({choices: [{delta: {content: '\n</think>\n\n'}}]})}\n\n`);
              }
              res.write('data: [DONE]\n\n');
              continue;
            }

            try {
              const data = JSON.parse(line.slice(6));
              if (data.choices?.[0]?.delta) {
                const delta = data.choices[0].delta;
                const reasoning = delta.reasoning_content || '';
                const content = delta.content || '';
                let mergedContent = '';

                if (reasoning) {
                  if (!isThinking) {
                    mergedContent += '<think>\n';
                    isThinking = true;
                  }
                  mergedContent += reasoning;
                }

                if (content) {
                  if (isThinking) {
                    mergedContent += '\n</think>\n\n';
                    isThinking = false;
                  }
                  mergedContent += content;
                }

                delta.content = mergedContent;
                delete delta.reasoning_content;
              }
              res.write(`data: ${JSON.stringify(data)}\n\n`);
            } catch (e) {
              // Silently ignore incomplete JSON chunks, they'll be caught in the buffer
            }
          }
        }
      }
      res.end();

    } else {
      // Non-streaming fallback logic
      const data = await response.json();
      data.choices.forEach(choice => {
        const msg = choice.message;
        if (msg.reasoning_content) {
          msg.content = `<think>\n${msg.reasoning_content}\n</think>\n\n${msg.content || ''}`;
          delete msg.reasoning_content;
        }
      });
      res.json(data);
    }

  } catch (error) {
    console.error('Proxy Error:', error.message);
    res.status(500).json({ error: { message: error.message, type: 'internal_server_error' } });
  }
});

// Explicitly bind to 0.0.0.0 so Railway routes traffic properly
app.listen(PORT, '0.0.0.0', () => {
  console.log(`Smart Proxy running on port ${PORT}`);
});
