// DOM Elements
const searchInput = document.getElementById('search-input');
const resultsContainer = document.getElementById('results-container');
const loadingSpinner = document.getElementById('loading-spinner');
const errorMessage = document.getElementById('error-message');
const resultsInfo = document.getElementById('results-info');
const emptyState = document.getElementById('empty-state');

// State
let debounceTimer;
const DEBOUNCE_DELAY = 500; // ms

// Event Listeners
searchInput.addEventListener('input', handleSearchInput);

function handleSearchInput(e) {
    clearTimeout(debounceTimer);
    const query = e.target.value.trim();

    if (!query) {
        showEmptyState();
        return;
    }

    // Debounce search
    debounceTimer = setTimeout(() => {
        performSearch(query);
    }, DEBOUNCE_DELAY);
}

async function performSearch(query) {
    try {
        // Show loading state
        showLoading();
        hideError();
        hideResultsInfo();

        // Make API request
        const response = await fetch('/api/search', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query }),
        });

        if (!response.ok) {
            const errorText = await response.text();
            throw new Error(errorText || `HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        
        hideLoading();
        displayResults(data);

    } catch (error) {
        console.error('Search error:', error);
        hideLoading();
        showError(`Search failed: ${error.message}`);
    }
}

function displayResults(data) {
    // Hide empty state
    emptyState.classList.add('hidden');

    if (data.total_results === 0) {
        resultsContainer.innerHTML = `
            <div class="empty-state" style="display: block;">
                <svg class="empty-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5">
                    <circle cx="11" cy="11" r="8"></circle>
                    <path d="m21 21-4.35-4.35"></path>
                    <line x1="11" y1="8" x2="11" y2="14"></line>
                    <line x1="11" y1="16" x2="11.01" y2="16"></line>
                </svg>
                <h2>No Results Found</h2>
                <p>Try different keywords or check your spelling</p>
            </div>
        `;
        hideResultsInfo();
        return;
    }

    // Show results info
    showResultsInfo(data.total_results, data.processing_time_ms);

    // Render result cards with highlighting
    resultsContainer.innerHTML = data.results.map((result, index) => {
        // Highlight terms in snippet
        const highlightedSnippet = highlightTerms(result.snippet, data.highlighted_terms || []);
        
        return `
            <div class="result-card">
                <h2 class="result-title">
                    <a href="${escapeHtml(result.url)}" target="_blank" rel="noopener noreferrer">
                        ${escapeHtml(result.title || 'Untitled Page')}
                    </a>
                </h2>
                <div class="result-url">${escapeHtml(result.url)}</div>
                <p class="result-snippet">${highlightedSnippet}</p>
                <div class="result-meta">
                    <span>📄 ID: ${escapeHtml(result.id.slice(0, 8))}...</span>
                    <span>🔍 Depth: ${result.depth}</span>
                </div>
            </div>
        `;
    }).join('');
}

// Highlight search terms in text
function highlightTerms(text, terms) {
    // Strip HTML tags and get plain text content
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = text;
    
    // Remove script and style elements to avoid showing their content
    const scripts = tempDiv.querySelectorAll('script, style');
    scripts.forEach(el => el.remove());
    
    const plainText = tempDiv.textContent || tempDiv.innerText || '';
    
    if (!terms || terms.length === 0) {
        return escapeHtml(plainText);
    }

    // Escape the text first for XSS protection
    let result = escapeHtml(plainText);
    
    // For each term, find and highlight it (case-insensitive)
    terms.forEach(term => {
        if (!term || term.trim().length === 0) return;
        
        // Remove punctuation from the term for better matching
        const cleanTerm = term.replace(/[^\w\s]/g, '').trim();
        if (cleanTerm.length === 0) return;
        
        // Escape special regex characters in the term
        const escapedTerm = cleanTerm.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
        
        // Use a more flexible pattern that matches the term with optional punctuation
        // This will match "running", "running.", "running,", etc.
        const regex = new RegExp(`(${escapedTerm})(?=[\\s.,;:!?<]|$)`, 'gi');
        
        result = result.replace(regex, '<mark>$1</mark>');
    });
    
    return result;
}

function showLoading() {
    loadingSpinner.classList.remove('hidden');
    resultsContainer.innerHTML = '';
}

function hideLoading() {
    loadingSpinner.classList.add('hidden');
}

function showError(message) {
    errorMessage.textContent = message;
    errorMessage.classList.remove('hidden');
    resultsContainer.innerHTML = '';
    emptyState.classList.add('hidden');
}

function hideError() {
    errorMessage.classList.add('hidden');
}

function showResultsInfo(count, timeMs) {
    resultsInfo.innerHTML = `Found <strong>${count}</strong> ${count === 1 ? 'result' : 'results'} in <strong>${timeMs}ms</strong>`;
    resultsInfo.classList.remove('hidden');
}

function hideResultsInfo() {
    resultsInfo.classList.add('hidden');
}

function showEmptyState() {
    resultsContainer.innerHTML = '';
    emptyState.classList.remove('hidden');
    hideError();
    hideResultsInfo();
}

// Utility function to escape HTML and prevent XSS
function escapeHtml(unsafe) {
    if (!unsafe) return '';
    return unsafe
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#039;");
}

// Initialize
showEmptyState();
