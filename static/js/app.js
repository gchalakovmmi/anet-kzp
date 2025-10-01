let currentProducts = [];
let currentSearchTerm = '';
let currentCategoryCode = '';
let isProcessingActive = false;
let statusCheckInterval = null;

// Sorting state
let currentSort = {
	column: null,
	direction: 'asc' // 'asc' or 'desc'
};

// Check processing status periodically
function checkProcessingStatus() {
	fetch('/api/processing-status')
		.then(response => response.json())
		.then(data => {
			// Update loading screen with the exact progress from backend
			document.getElementById('loadingProgress').style.width = data.progress + '%';
			document.getElementById('progressPercent').textContent = data.progress;
			document.getElementById('currentMarket').textContent = data.current_market || '-';
			document.getElementById('loadingMessage').textContent = data.message;

			// Handle errors
			if (data.error) {
				document.getElementById('loadingTitle').textContent = 'Грешка при обработка';
				document.getElementById('errorText').textContent = data.error;
				document.getElementById('errorMessage').style.display = 'block';
				document.getElementById('loadingScreen').style.display = 'flex';
				isProcessingActive = false;
				// Re-enable processing button on error
				document.getElementById('startProcessingBtn').disabled = false;
				document.getElementById('startProcessingBtn').innerHTML = '<i class="bi bi-play-fill"></i> Обработка на данни';
				return;
			}

			// Show/hide loading screen based on processing status
			if (data.is_processing) {
				isProcessingActive = true;
				document.getElementById('loadingScreen').style.display = 'flex';
				document.getElementById('mainContent').style.display = 'none';
				// Continue checking status every 2 seconds during processing
				if (statusCheckInterval) {
					clearInterval(statusCheckInterval);
				}
				statusCheckInterval = setInterval(checkProcessingStatus, 2000);
			} else {
				isProcessingActive = false;
				document.getElementById('loadingScreen').style.display = 'none';
				document.getElementById('mainContent').style.display = 'flex';
				// Re-enable processing button when processing is complete
				document.getElementById('startProcessingBtn').disabled = false;
				document.getElementById('startProcessingBtn').innerHTML = '<i class="bi bi-play-fill"></i> Обработка на данни';
				
				// If database is ready, enable export and stop checking
				if (data.database_ready) {
					document.getElementById('exportCsv').disabled = false;
					// Stop checking status after processing is complete
					if (statusCheckInterval) {
						clearInterval(statusCheckInterval);
						statusCheckInterval = null;
					}
					// Only show search prompt if we don't have current search results AND no category is selected
					if (currentSearchTerm === '' && currentCategoryCode === '') {
						showSearchPrompt();
					}
				} else {
					document.getElementById('exportCsv').disabled = true;
					// Check status every 10 seconds when not processing but database not ready
					if (statusCheckInterval) {
						clearInterval(statusCheckInterval);
					}
					statusCheckInterval = setInterval(checkProcessingStatus, 10000);
				}
			}
		})
		.catch(error => {
			console.error('Error checking status:', error);
			// Continue checking even on error, but less frequently (every 30 seconds)
			if (statusCheckInterval) {
				clearInterval(statusCheckInterval);
			}
			statusCheckInterval = setInterval(checkProcessingStatus, 30000);
		});
}

// Start manual processing
function startManualProcessing() {
	// Disable the button to prevent multiple clicks
	const processingBtn = document.getElementById('startProcessingBtn');
	processingBtn.disabled = true;
	processingBtn.innerHTML = '<i class="bi bi-hourglass-split"></i> Започва се...';
	
	fetch('/api/start-processing', {
		method: 'POST',
		headers: {
			'Content-Type': 'application/json',
		}
	})
	.then(response => response.json())
	.then(data => {
		if (data.success) {
			// Clear current search when starting new processing
			currentSearchTerm = '';
			currentCategoryCode = '';
			document.getElementById('searchInput').value = '';
			document.getElementById('categorySelect').value = '';
			// Start checking status to show progress
			checkProcessingStatus();
		} else {
			alert('Грешка при стартиране на обработката: ' + data.error);
			// Re-enable the button on error
			processingBtn.disabled = false;
			processingBtn.innerHTML = '<i class="bi bi-play-fill"></i> Обработка на данни';
		}
	})
	.catch(error => {
		console.error('Error starting processing:', error);
		alert('Грешка при стартиране на обработката.');
		// Re-enable the button on error
		processingBtn.disabled = false;
		processingBtn.innerHTML = '<i class="bi bi-play-fill"></i> Обработка на данни';
	});
}

// Show search prompt when no search term is entered and no category is selected
function showSearchPrompt() {
	// Only show search prompt if we're not in the middle of processing and no category is selected
	if (!isProcessingActive && currentCategoryCode === '') {
		const tbody = document.getElementById('productsTableBody');
		const noResults = document.getElementById('noResults');
		const searchPrompt = document.getElementById('searchPrompt');
		tbody.innerHTML = '';
		noResults.style.display = 'none';
		searchPrompt.style.display = 'block';
	}
}

// Load products from the server
function loadProducts(searchTerm = '') {
	currentSearchTerm = searchTerm;
	currentCategoryCode = '';
	let url = '/api/search';
	if (searchTerm) {
		url += '?q=' + encodeURIComponent(searchTerm);
	}
	fetch(url)
		.then(response => response.json())
		.then(data => {
			if (data.error) {
				console.error('Error loading products:', data.error);
				return;
			}
			currentProducts = data.products || [];
			// Reset sorting when loading new data
			resetSorting();
			displayProducts(currentProducts, data.search_term);
		})
		.catch(error => {
			console.error('Error loading products:', error);
		});
}

// Load products by category
function loadProductsByCategory(categoryCode) {
	if (!categoryCode) {
		currentCategoryCode = '';
		showSearchPrompt();
		return;
	}
	currentCategoryCode = categoryCode;
	let url = '/api/products-by-category?category_code=' + encodeURIComponent(categoryCode);
	fetch(url)
		.then(response => response.json())
		.then(data => {
			if (data.error) {
				console.error('Error loading products by category:', data.error);
				return;
			}
			currentProducts = data.products || [];
			// Reset sorting when loading new data
			resetSorting();
			displayProducts(currentProducts, '');
			// Clear the search input when loading by category
			document.getElementById('searchInput').value = '';
			currentSearchTerm = '';
		})
		.catch(error => {
			console.error('Error loading products by category:', error);
		});
}

// Sorting functions
function sortProducts(column, direction) {
	if (!currentProducts.length) return;

	const sortedProducts = [...currentProducts].sort((a, b) => {
		let aValue = a[column];
		let bValue = b[column];

		// Handle null/undefined values
		if (aValue === null || aValue === undefined) aValue = '';
		if (bValue === null || bValue === undefined) bValue = '';

		// Convert to string for comparison
		aValue = String(aValue).toLowerCase();
		bValue = String(bValue).toLowerCase();

		// For numeric columns, convert to numbers if possible
		if (column === 'item_retail_price' || column === 'item_promotional_price') {
			const aNum = parseFloat(aValue);
			const bNum = parseFloat(bValue);
			if (!isNaN(aNum) && !isNaN(bNum)) {
				aValue = aNum;
				bValue = bNum;
			}
		}

		let comparison = 0;
		if (aValue < bValue) comparison = -1;
		if (aValue > bValue) comparison = 1;

		return direction === 'desc' ? comparison * -1 : comparison;
	});

	return sortedProducts;
}

function updateSortIndicators(column, direction) {
	// Remove all sort indicators
	document.querySelectorAll('.sortable').forEach(th => {
		th.classList.remove('active', 'asc', 'desc');
	});

	// Add indicator to current sort column
	const currentTh = document.querySelector(`.sortable[data-column="${column}"]`);
	if (currentTh) {
		currentTh.classList.add('active', direction);
	}
}

function resetSorting() {
	currentSort.column = null;
	currentSort.direction = 'asc';
	updateSortIndicators(null, 'asc');
}

function handleSort(column) {
	if (currentSort.column === column) {
		// Toggle direction if same column
		currentSort.direction = currentSort.direction === 'asc' ? 'desc' : 'asc';
	} else {
		// New column, default to ascending
		currentSort.column = column;
		currentSort.direction = 'asc';
	}

	updateSortIndicators(column, currentSort.direction);
	
	// Sort and display products
	const sortedProducts = sortProducts(column, currentSort.direction);
	displayProducts(sortedProducts, currentSearchTerm);
}

// Display products in the table
function displayProducts(products, searchTerm) {
	const tbody = document.getElementById('productsTableBody');
	const noResults = document.getElementById('noResults');
	const searchPrompt = document.getElementById('searchPrompt');

	// Hide both prompts initially
	noResults.style.display = 'none';
	searchPrompt.style.display = 'none';

	if (products.length === 0) {
		tbody.innerHTML = '';
		if ((!searchTerm || searchTerm.trim() === '') && currentCategoryCode === '') {
			// Show search prompt when no search term and no category
			searchPrompt.style.display = 'block';
		} else {
			// Show no results when search term returns nothing or category has no products
			noResults.style.display = 'block';
		}
		return;
	}

	tbody.innerHTML = products.map(product => {
		const isCategorized = product.item_kzp_category_code && product.item_kzp_category_code !== '';
		const rowClass = isCategorized ? 'categorized-row' : '';
		const categoryDisplay = isCategorized ?
			`<span class="category-badge">${product.item_kzp_category_name}</span>` : '';

		// Format prices to show 2 decimal places
		const retailPrice = product.item_retail_price ? 
			parseFloat(product.item_retail_price).toFixed(2) : '';
		const promotionalPrice = product.item_promotional_price ? 
			parseFloat(product.item_promotional_price).toFixed(2) : '';

		return `
		<tr class="${rowClass}" data-product-id="${product.id}">
			<td>
				<div class="form-check">
					<input class="form-check-input item-checkbox" type="checkbox" name="item" value="${product.id}">
				</div>
			</td>
			<td>${product.settlement || ''}</td>
			<td>${product.market_name || ''}</td>
			<td>${product.item_name || ''}</td>
			<td>${product.item_code || ''}</td>
			<td>${categoryDisplay}</td>
			<td>${retailPrice}</td>
			<td>${promotionalPrice}</td>
		</tr>
		`;
	}).join('');

	updateSelectedCount();
}

// Update selected count function
function updateSelectedCount() {
	const selectedCheckboxes = document.querySelectorAll('.item-checkbox:checked');
	document.getElementById('selectedCount').textContent = selectedCheckboxes.length;
	
	// Update select all checkbox state
	const totalCheckboxes = document.querySelectorAll('.item-checkbox');
	document.getElementById('selectAll').checked = selectedCheckboxes.length > 0 && selectedCheckboxes.length === totalCheckboxes.length;
}

// Clear search function
function clearSearch() {
	document.getElementById('searchInput').value = '';
	document.getElementById('categorySelect').value = '';
	currentSearchTerm = '';
	currentCategoryCode = '';
	showSearchPrompt();
}

// Initialize the main application after loading is complete
function initializeApplication() {
	// Add click handlers for sortable columns
	document.querySelectorAll('.sortable').forEach(th => {
		th.addEventListener('click', function() {
			const column = this.getAttribute('data-column');
			handleSort(column);
		});
	});

	// Category selection change event
	document.getElementById('categorySelect').addEventListener('change', function() {
		const categoryCode = this.value;
		if (categoryCode) {
			loadProductsByCategory(categoryCode);
		} else {
			// If category is cleared, show search prompt
			currentCategoryCode = '';
			showSearchPrompt();
		}
	});

	// Clear search button
	document.getElementById('clearSearchButton').addEventListener('click', clearSearch);

	// Use event delegation for checkbox changes
	document.getElementById('productsTableBody').addEventListener('change', function(e) {
		if (e.target && e.target.classList.contains('item-checkbox')) {
			updateSelectedCount();
		}
	});

	// Select all functionality
	document.getElementById('selectAll').addEventListener('change', function() {
		const checkboxes = document.querySelectorAll('.item-checkbox');
		checkboxes.forEach(checkbox => {
			checkbox.checked = this.checked;
		});
		updateSelectedCount();
	});

	// Clear selection button
	document.getElementById('clearSelection').addEventListener('click', function() {
		document.getElementById('selectAll').checked = false;
		const checkboxes = document.querySelectorAll('.item-checkbox');
		checkboxes.forEach(checkbox => {
			checkbox.checked = false;
		});
		updateSelectedCount();
	});

	// Add to category button
	document.getElementById('addToCategory').addEventListener('click', function() {
		const selectedCategory = document.getElementById('categorySelect').value;
		if (!selectedCategory) {
			alert('Моля, изберете категория преди да добавите продукти.');
			return;
		}

		const selectedCheckboxes = document.querySelectorAll('.item-checkbox:checked');
		const selectedProductIds = Array.from(selectedCheckboxes).map(cb => cb.value);

		if (selectedProductIds.length === 0) {
			alert('Моля, изберете поне един продукт преди да го добавите към категория.');
			return;
		}

		// Send request to update categories
		fetch('/api/update-category', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				product_ids: selectedProductIds,
				category_code: selectedCategory
			})
		})
		.then(response => response.json())
		.then(data => {
			if (data.success) {
				// Reload current view to show updated categories
				if (currentCategoryCode) {
					loadProductsByCategory(currentCategoryCode);
				} else {
					const currentSearch = document.getElementById('searchInput').value.trim();
					loadProducts(currentSearch);
				}
				// Clear selection
				document.getElementById('selectAll').checked = false;
				selectedCheckboxes.forEach(checkbox => {
					checkbox.checked = false;
				});
				updateSelectedCount();
			} else {
				alert('Грешка при добавяне на категория: ' + data.error);
			}
		})
		.catch(error => {
			console.error('Error updating category:', error);
			alert('Грешка при добавяне на категория.');
		});
	});

	// Remove from category button
	document.getElementById('removeFromCategory').addEventListener('click', function() {
		const selectedCheckboxes = document.querySelectorAll('.item-checkbox:checked');
		const selectedProductIds = Array.from(selectedCheckboxes).map(cb => cb.value);

		if (selectedProductIds.length === 0) {
			alert('Моля, изберете поне един продукт преди да го премахнете от категория.');
			return;
		}

		// Send request to remove categories
		fetch('/api/remove-category', {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json',
			},
			body: JSON.stringify({
				product_ids: selectedProductIds
			})
		})
		.then(response => response.json())
		.then(data => {
			if (data.success) {
				// Reload current view to show updated categories
				if (currentCategoryCode) {
					loadProductsByCategory(currentCategoryCode);
				} else {
					const currentSearch = document.getElementById('searchInput').value.trim();
					loadProducts(currentSearch);
				}
				// Clear selection
				document.getElementById('selectAll').checked = false;
				selectedCheckboxes.forEach(checkbox => {
					checkbox.checked = false;
				});
				updateSelectedCount();
			} else {
				alert('Грешка при премахване на категория: ' + data.error);
			}
		})
		.catch(error => {
			console.error('Error removing category:', error);
			alert('Грешка при премахване на категория.');
		});
	});

	// Search functionality - DO NOT clear category when searching
	document.getElementById('searchButton').addEventListener('click', function() {
		const searchTerm = document.getElementById('searchInput').value.trim();
		if (searchTerm) {
			loadProducts(searchTerm);
		} else {
			showSearchPrompt();
		}
	});

	// Search on Enter key - DO NOT clear category when searching
	document.getElementById('searchInput').addEventListener('keypress', function(e) {
		if (e.key === 'Enter') {
			document.getElementById('searchButton').click();
		}
	});

	// Export CSV functionality
	document.getElementById('exportCsv').addEventListener('click', function() {
		window.open('/api/export-csv', '_blank');
	});
}

// Start checking processing status when page loads
document.addEventListener('DOMContentLoaded', function() {
	// Add event listener for manual processing button
	document.getElementById('startProcessingBtn').addEventListener('click', startManualProcessing);

	// Initialize the application
	initializeApplication();

	// Show initial search prompt
	showSearchPrompt();

	// Start checking processing status
	checkProcessingStatus();
});
