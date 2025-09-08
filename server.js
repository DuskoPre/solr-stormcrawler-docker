const express = require("express");
const path = require("path");
const cors = require("cors");

const PORT = process.env.PORT || 4000;

const app = express();
app.use(cors());

// Serve static files from the current directory
app.use(express.static(__dirname));

// Serve index.html for the root route
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.listen(PORT, ()=> console.log(`server.js listening on :${PORT}`));
