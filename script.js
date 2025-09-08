const searchBox = document.getElementById('search-box');
const searchButton = document.getElementById('search-button');
const resultsContainer = document.getElementById('results-container');

searchButton.addEventListener('click', async () => {
    const query = searchBox.value;
    if (!query) return;

    try {
        const response = await fetch(`http://localhost:4000/api/solr/select?q=${encodeURIComponent(query)}`);
        const data = await response.json();
        displayResults(data.response.docs);
    } catch (error) {
        console.error('Error fetching search results:', error);
        resultsContainer.innerHTML = '<p>Error fetching results.</p>';
    }
});

function displayResults(docs) {
    if (docs.length === 0) {
        resultsContainer.innerHTML = '<p>No results found.</p>';
        return;
    }

    const list = document.createElement('ul');
    docs.forEach(doc => {
        const item = document.createElement('li');
        const link = document.createElement('a');
        link.href = doc.url;
        link.textContent = doc.title || doc.url;
        link.target = '_blank';
        item.appendChild(link);
        list.appendChild(item);
    });
    resultsContainer.innerHTML = '';
    resultsContainer.appendChild(list);
}
