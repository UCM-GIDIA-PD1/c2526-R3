import './style.css';
import mapboxgl from 'mapbox-gl';
import MapboxGeocoder from '@mapbox/mapbox-gl-geocoder';

// Fetch token from .env
mapboxgl.accessToken = import.meta.env.VITE_MAPBOX_TOKEN;

// Default to Madrid initially
const defaultCenter = [-3.703790, 40.416775];

// Europe bounding box: [WestLng, SouthLat], [EastLng, NorthLat]
const europeBounds = [
    [-31.8, 27.6], // Southwest (Includes Canary Islands and Azores)
    [45.0, 71.2]     // Northeast (Northern Scandinavia and part of Eastern Europe)
];

const map = new mapboxgl.Map({
    container: 'map',
    style: 'mapbox://styles/mapbox/satellite-streets-v12',
    center: defaultCenter, 
    zoom: 5,
    pitch: 0,
    bearing: 0,
    maxBounds: europeBounds // Restrict map panning to Europe
});

// Map controls
map.addControl(new mapboxgl.NavigationControl(), 'bottom-right');

// Wait for style load
map.on('style.load', () => {
    map.addSource('mapbox-dem', {
        'type': 'raster-dem',
        'url': 'mapbox://mapbox.mapbox-terrain-dem-v1',
        'tileSize': 512,
        'maxzoom': 14
    });
    map.setTerrain({ 'source': 'mapbox-dem', 'exaggeration': 1.5 });
    map.addLayer({
        'id': 'sky',
        'type': 'sky',
        'paint': {
            'sky-type': 'atmosphere',
            'sky-atmosphere-sun': [0.0, 0.0],
            'sky-atmosphere-sun-intensity': 15
        }
    });
});

// Setup Geocoder
const geocoder = new MapboxGeocoder({
    accessToken: mapboxgl.accessToken,
    mapboxgl: mapboxgl,
    placeholder: 'Buscar ubicación o introducir Lat, Lon...',
    marker: false,
    bbox: [-31.8, 27.6, 45.0, 71.2] // Restrict search results to Europe bounds
});
document.getElementById('geocoder-container').appendChild(geocoder.onAdd(map));

let currentMarker = null;

// UI Elements
const tooltip = document.getElementById('map-tooltip');
const headerCoords = document.getElementById('header-coords');
const coordsInput = document.getElementById('coords-input');
const dateInput = document.getElementById('date-input');
const panel = document.getElementById('prediction-panel');
const closeBtn = document.getElementById('close-panel-btn');
const tabRiesgo = document.getElementById('tab-riesgo');
const tabFrp = document.getElementById('tab-frp');
const resultsContainer = document.getElementById('results-container');
const actionBtn = document.getElementById('action-btn');
const backBtn = document.getElementById('back-btn');
const openPanelBtn = document.getElementById('open-panel-btn');

let previousView = null;

// Set today's date implicitly and restrict past dates
const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');
const formattedToday = `${year}-${month}-${day}`;

dateInput.value = formattedToday;
dateInput.min = formattedToday;

let activeTab = 'riesgo'; // 'riesgo' or 'frp'
let hasPrediction = false;

// Valid European country ISO codes
const europeanCountries = [
    'ad', 'al', 'at', 'ba', 'be', 'bg', 'by', 'ch', 'cy', 'cz', 'de', 'dk', 'ee', 
    'es', 'fi', 'fr', 'gb', 'gr', 'hr', 'hu', 'ie', 'is', 'it', 'li', 'lt', 'lu', 
    'lv', 'mc', 'md', 'me', 'mk', 'mt', 'nl', 'no', 'pl', 'pt', 'ro', 'rs', 'ru', 
    'se', 'si', 'sk', 'sm', 'ua', 'va', 'xk'
];

// Handle map clicks
map.on('click', async (e) => {
    const lng = e.lngLat.lng;
    const lat = e.lngLat.lat;

    try {
        // Check country via reverse geocoding
        const response = await fetch(`https://api.mapbox.com/geocoding/v5/mapbox.places/${lng},${lat}.json?types=country&access_token=${mapboxgl.accessToken}`);
        const data = await response.json();
        
        if (data.features && data.features.length > 0) {
            const countryCode = data.features[0].properties.short_code.toLowerCase();
            if (!europeanCountries.includes(countryCode)) {
                alert("Selección fuera de Europa. Por favor, haz clic dentro de territorio europeo.");
                return;
            }
        } else {
             // No country found (e.g. ocean)
             alert("Ubicación en el mar o no válida. Por favor, selecciona un punto en tierra dentro de Europa.");
             return;
        }
    } catch (err) {
        console.error("Geocoding error:", err);
    }

    tooltip.style.opacity = '0'; // Hide tooltip on first click
    saveCurrentView();
    openPanel();
    setCoordinates(lng, lat);

    map.flyTo({
        center: [lng, lat],
        zoom: 14,
        pitch: 65,
        duration: 1500, // Smooth transition duration
        essential: true
    });
});

// Handle geocoder result
geocoder.on('result', (e) => {
    tooltip.style.opacity = '0';
    saveCurrentView();
    openPanel();
    setCoordinates(e.result.center[0], e.result.center[1]);
});

function saveCurrentView() {
    previousView = {
        center: map.getCenter(),
        zoom: map.getZoom(),
        pitch: map.getPitch(),
        bearing: map.getBearing()
    };
    backBtn.classList.remove('hidden');
}

function openPanel() {
    panel.classList.remove('is-hidden');
    openPanelBtn.classList.add('hidden');
}

function closePanel() {
    panel.classList.add('is-hidden');
    openPanelBtn.classList.remove('hidden');
}

backBtn.addEventListener('click', () => {
    if (!previousView) return;
    map.flyTo({
        center: previousView.center,
        zoom: previousView.zoom,
        pitch: previousView.pitch,
        bearing: previousView.bearing,
        duration: 1500,
        essential: true
    });
    backBtn.classList.add('hidden');
    previousView = null;
});

openPanelBtn.addEventListener('click', () => {
    openPanel();
});

// Set coordinates function
function setCoordinates(lng, lat) {
    const formatLng = lng.toFixed(4);
    const formatLat = lat.toFixed(4);
    const lngDir = lng >= 0 ? 'E' : 'W';
    const latDir = lat >= 0 ? 'N' : 'S';
    
    const coordString = `${Math.abs(formatLat)}° ${latDir}, ${Math.abs(formatLng)}° ${lngDir}`;
    
    headerCoords.textContent = coordString;
    coordsInput.value = coordString;

    if (currentMarker) currentMarker.remove();
    currentMarker = new mapboxgl.Marker({ color: '#f97316' })
        .setLngLat([lng, lat])
        .addTo(map);

    // Reset prediction state when a new point is selected
    hasPrediction = false;
    updateUIState();
}

// Interactivity
closeBtn.addEventListener('click', () => {
    closePanel();
});

tabRiesgo.addEventListener('click', () => {
    activeTab = 'riesgo';
    tabRiesgo.classList.add('active');
    tabFrp.classList.remove('active');
    if (hasPrediction) generateMockPrediction();
});

tabFrp.addEventListener('click', () => {
    activeTab = 'frp';
    tabFrp.classList.add('active');
    tabRiesgo.classList.remove('active');
    if (hasPrediction) generateMockPrediction();
});

actionBtn.addEventListener('click', () => {
    if(!coordsInput.value) {
        alert("Por favor, selecciona una ubicación en el mapa primero.");
        return;
    }
    hasPrediction = true;
    updateUIState();
    generateMockPrediction();
});

function updateUIState() {
    if (hasPrediction) {
        actionBtn.textContent = 'Recalcular predicción';
        actionBtn.classList.add('outline-btn');
        actionBtn.classList.remove('primary-btn');
    } else {
        resultsContainer.innerHTML = '';
        actionBtn.textContent = 'Generar predicción';
        actionBtn.classList.remove('outline-btn');
        actionBtn.classList.add('primary-btn');
    }
}

function generateMockPrediction() {
    if (activeTab === 'riesgo') {
        resultsContainer.innerHTML = `
            <div class="result-section">
                <div class="result-header">Resultado de la Predicción</div>
                <div class="risk-probability">PROBABILIDAD DE INCENDIO: 95%</div>
                
                <div class="factors-title">Principales Factores Determinantes (Impacto)</div>
                
                <div class="factor-item">
                    <div class="factor-header">
                        <span class="factor-icon">🌡️</span> Alta Temperatura (34ºC)
                    </div>
                    <div class="progress-bg"><div class="progress-bar pb-red"></div></div>
                </div>
                
                <div class="factor-item">
                    <div class="factor-header">
                        <span class="factor-icon">💧</span> Baja Humedad (18%)
                    </div>
                    <div class="progress-bg"><div class="progress-bar pb-orange"></div></div>
                </div>
                
                <div class="factor-item">
                    <div class="factor-header">
                        <span class="factor-icon">💨</span> Viento Moderado (20 km/h)
                    </div>
                    <div class="progress-bg"><div class="progress-bar pb-yellow"></div></div>
                </div>
                
                <div class="factor-item">
                    <div class="factor-header">
                        <span class="factor-icon">🌿</span> Sequedad de la Vegetación (Extrema)
                    </div>
                    <div class="progress-bg"><div class="progress-bar pb-extreme"></div></div>
                </div>
            </div>
        `;
    } else {
        resultsContainer.innerHTML = `
            <div class="result-section">
                <div class="result-header">Resultado de la Predicción de FRP</div>
                <div class="frp-title">FRP ESTIMADO:</div>
                <div class="frp-value">87.3 MW</div>
                <div class="frp-subtitle">Potencia Radiativa del Fuego Estimada</div>
            </div>
        `;
    }
}

// Current Location Button
document.getElementById('target-btn').addEventListener('click', () => {
    if (navigator.geolocation) {
        navigator.geolocation.getCurrentPosition(
            (position) => {
                const lng = position.coords.longitude;
                const lat = position.coords.latitude;
                saveCurrentView();
                map.flyTo({ center: [lng, lat], zoom: 12 });
                setCoordinates(lng, lat);
                openPanel();
            },
            () => {
                alert("No se pudo obtener la ubicación actual.");
            }
        );
    }
});
