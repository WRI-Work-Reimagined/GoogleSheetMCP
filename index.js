const express = require('express');
const axios = require('axios');
const { parse } = require('csv-parse/sync');

const app = express();
app.use(express.json());

app.post('/gsheets_export_csv', async (req, res) => {
  const { url } = req.body;

  try {
    const response = await axios.get(url);
    const records = parse(response.data, {
      columns: true,
      skip_empty_lines: true
    });

    res.json({ rows: records });
  } catch (error) {
    console.error('Error fetching CSV:', error.message);
    res.status(500).json({ error: 'Failed to fetch or parse CSV' });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`MCP server running on port ${PORT}`));
