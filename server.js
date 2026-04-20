const express = require('express');
const cors = require('cors');
const axios = require('axios');

const app = express();
const PORT = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());

const NIM_API_BASE = 'https://integrate.api.nvidia.com/v1';
const NIM_API_KEY = process.env.NIM_API_KEY;

// Fail fast if the API key is missing
if (!NIM_API_KEY) {
  console.error('FATAL: NIM_API_KEY environment variable is missing.');
  process.exit(1);
}

// Handle non-POST requests (health check for Railway)
app.all('/', async (req, res) => {
  return res.json({ status: 'ok', service: 'OpenAI to NVIDIA NIM Proxy' });
});

// Scope strictly to chat completions to avoid forwarding bad paths
app.post(['/v1/chat/completions', '/chat/completions'], async (req, res) => {
  try {
    const { model, messages, temperature, max_tokens, stream, top_p, frequency_penalty, presence_penalty } = req.body;
    
    console.log(`[REQUEST] path=${req.path} model=${model} stream=${stream}`);

    const nimRequest = {
      model,
      messages,
      stream: stream || false,
      ...(temperature !== undefined && { temperature }),
      ...(max_tokens !== undefined && { max_tokens }),
      ...(top_p !== undefined && { top_p }),
      ...(frequency_penalty !== undefined && { frequency_penalty }),
      ...(presence_penalty !== undefined && { presence_penalty }),
    };

    const response = await axios.post(`${NIM_API_BASE}/chat/completions`, nimRequest, {
      headers: {
        'Authorization': `Bearer ${NIM_API_KEY}`,
        'Content-Type': 'application/json'
      },
      responseType: stream ? 'stream' : 'json',
      timeout: 60000
    });

    console.log(`[SUCCESS] status=${response.status}`);

    if (stream) {
      res.setHeader('Content-Type', 'text/event-stream');
      res.setHeader('Cache-Control', 'no-cache');
      res.setHeader('Connection', 'keep-alive');
      
      let buffer = '';
      let isThinking = false; // Tracks if we need to close a think block
      
      response.data.on('data', (chunk) => {
        buffer += chunk.toString();
        const lines = buffer.split('\n');
        buffer = lines.pop() || ''; 
        
        lines.forEach(line => {
          if (line.startsWith('data: ')) {
            if (line.includes('[DONE]')) {
              // Safety catch: close the tag if the stream ends while thinking
              if (isThinking) {
                res.write(`data: ${JSON.stringify({choices: [{delta: {content: '\n</think>\n\n'}}]})}\n\n`);
              }
              res.write(line + '\n\n');
              return;
            }
            try {
              const data = JSON.parse(line.slice(6));
              if (data.choices?.[0]?.delta) {
                const delta = data.choices[0].delta;
                
                const reasoning = delta.reasoning_content || '';
                const content = delta.content || '';
                let mergedContent = '';

                // 1. If we receive reasoning and haven't opened the think tag yet
                if (reasoning) {
                  if (!isThinking) {
                    mergedContent += '<think>\n';
                    isThinking = true;
                  }
                  mergedContent += reasoning;
                }

                // 2. If we receive standard content and the think tag is still open
                if (content) {
                  if (isThinking) {
                    mergedContent += '\n</think>\n\n';
                    isThinking = false;
                  }
                  mergedContent += content;
                }

                // 3. Output the properly tagged stream and delete the non-standard field
                delta.content = mergedContent;
                delete delta.reasoning_content;
              }
              res.write(`data: ${JSON.stringify(data)}\n\n`);
            } catch (e) {
              res.write(line + '\n');
            }
          }
        });
      });
      
      response.data.on('end', () => res.end());
      response.data.on('error', (err) => {
        console.error('Stream error:', err);
        res.end();
      });
      
    } else {
      // Non-streaming fallback: format reasoning into standard content
      const modifiedChoices = response.data.choices.map(choice => {
        const msg = choice.message;
        let mergedContent = '';
        
        if (msg.reasoning_content) {
          mergedContent += `<think>\n${msg.reasoning_content}\n</think>\n\n`;
          delete msg.reasoning_content; // Clean up
        }
        
        if (msg.content) {
          mergedContent += msg.content;
        }
        
        return {
          index: choice.index,
          message: {
            role: msg.role,
            content: mergedContent || ''
          },
          finish_reason: choice.finish_reason
        };
      });

      res.json({
        id: `chatcmpl-${Date.now()}`,
        object: 'chat.completion',
        created: Math.floor(Date.now() / 1000),
        model: model,
        choices: modifiedChoices,
        usage: response.data.usage || { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 }
      });
    }
  } catch (error) {
    console.error('Proxy error:', error.message, error.response?.data ? JSON.stringify(error.response.data) : '');
    res.status(error.response?.status || 500).json({
      error: {
        message: error.message,
        type: 'invalid_request_error',
        code: error.response?.status || 500
      }
    });
  }
});

app.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT}`);
  console.log(`NIM_API_KEY set: ${!!NIM_API_KEY}`);
});
