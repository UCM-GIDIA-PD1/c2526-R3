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
    addCustomLayers();
});

function addCustomLayers() {
    if (!map.getSource('mapbox-dem')) {
        map.addSource('mapbox-dem', {
            'type': 'raster-dem',
            'url': 'mapbox://mapbox.mapbox-terrain-dem-v1',
            'tileSize': 512,
            'maxzoom': 14
        });
    }
    map.setTerrain({ 'source': 'mapbox-dem', 'exaggeration': 1.5 });
    
    if (!map.getLayer('sky')) {
        map.addLayer({
            'id': 'sky',
            'type': 'sky',
            'paint': {
                'sky-type': 'atmosphere',
                'sky-atmosphere-sun': [0.0, 0.0],
                'sky-atmosphere-sun-intensity': 15
            }
        });
    }

    // Añadir fuente de datos de incendios históricos
    if (!map.getSource('historical-fires')) {
        map.addSource('historical-fires', {
            type: 'geojson',
            data: '/fires.geojson', // Se cargará dinámicamente de public/
            cluster: true,
            clusterMaxZoom: 14,
            clusterRadius: 50
        });
    }

    const historyVisibility = appMode === 'history' ? 'visible' : 'none';

    if (!map.getLayer('fires-heatmap')) {
        map.addLayer({
            id: 'fires-heatmap',
            type: 'heatmap',
            source: 'historical-fires',
            paint: {
                'heatmap-weight': [
                    'interpolate',
                    ['linear'],
                    ['get', 'point_count'],
                    0, 1,
                    500, 3
                ],
                'heatmap-intensity': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, 1,
                    15, 3
                ],
                'heatmap-color': [
                    'interpolate',
                    ['linear'],
                    ['heatmap-density'],
                    0, 'rgba(0, 255, 0, 0)',
                    0.2, 'rgba(132, 204, 34, 0.5)', 
                    0.5, 'rgba(234, 179, 8, 0.6)',  
                    0.8, 'rgba(249, 115, 22, 0.7)', 
                    1, 'rgba(239, 68, 68, 0.8)'     
                ],
                'heatmap-radius': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, 15,
                    9, 30
                ],
                'heatmap-opacity': 0.8
            },
            layout: { visibility: historyVisibility }
        });
    }

    if (!map.getLayer('clusters')) {
        map.addLayer({
            id: 'clusters',
            type: 'circle',
            source: 'historical-fires',
            filter: ['has', 'point_count'],
            paint: {
                'circle-color': [
                    'step',
                    ['get', 'point_count'],
                    'rgba(132, 204, 34, 0.6)', 20, 
                    'rgba(250, 204, 21, 0.6)', 50, 
                    'rgba(249, 115, 22, 0.6)', 150, 
                    'rgba(239, 68, 68, 0.6)'
                ],
                'circle-radius': [
                    'step',
                    ['get', 'point_count'],
                    16, 20,
                    20, 50,
                    24, 150,
                    28
                ],
                'circle-stroke-width': 12,
                'circle-stroke-color': [
                    'step',
                    ['get', 'point_count'],
                    'rgba(132, 204, 34, 0.25)', 20,
                    'rgba(250, 204, 21, 0.25)', 50,
                    'rgba(249, 115, 22, 0.25)', 150,
                    'rgba(239, 68, 68, 0.25)'
                ]
            },
            layout: { visibility: historyVisibility }
        });
    }

    if (!map.getLayer('cluster-count')) {
        map.addLayer({
            id: 'cluster-count',
            type: 'symbol',
            source: 'historical-fires',
            filter: ['has', 'point_count'],
            layout: {
                'text-field': '{point_count_abbreviated}',
                'text-font': ['DIN Offc Pro Medium', 'Arial Unicode MS Bold'],
                'text-size': 14,
                visibility: historyVisibility
            },
            paint: {
                'text-color': '#000000',
                'text-halo-color': 'rgba(255, 255, 255, 0.8)',
                'text-halo-width': 1.5,
                'text-halo-blur': 0.5
            }
        });
    }

    if (!map.getLayer('unclustered-point')) {
        map.addLayer({
            id: 'unclustered-point',
            type: 'circle',
            source: 'historical-fires',
            filter: ['!', ['has', 'point_count']],
            paint: {
                'circle-color': 'rgba(239, 68, 68, 0.8)',
                'circle-radius': 6,
                'circle-stroke-width': 6,
                'circle-stroke-color': 'rgba(239, 68, 68, 0.25)'
            },
            layout: { visibility: historyVisibility }
        });
    }
}

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

// Application Mode: 'prediction' | 'history'
let appMode = 'prediction';

// Mode DOM Elements
const btnModePrediction = document.getElementById('mode-prediction-btn');
const btnModeHistory = document.getElementById('mode-history-btn');
const heatmapToggleWrapper = document.getElementById('heatmap-toggle-wrapper');
const heatmapToggle = document.getElementById('heatmap-toggle');

// Prediction UI Elements
const headerCoords = document.getElementById('header-coords');
const coordsInput = document.getElementById('coords-input');
const dateInput = document.getElementById('date-input');
const panel = document.getElementById('prediction-panel');
const closeBtn = document.getElementById('close-panel-btn');
const tabRiesgo = document.getElementById('tab-riesgo');
const tabFrp = document.getElementById('tab-frp');
const resultsContainer = document.getElementById('results-container');
const actionBtn = document.getElementById('action-btn');

// History UI Elements
const historyPanel = document.getElementById('history-panel');
const closeHistoryBtn = document.getElementById('close-history-btn');
const historyContent = document.getElementById('history-content-container');
const historyHeaderCoords = document.getElementById('history-header-coords');

const backBtn = document.getElementById('back-btn');
const openPanelBtn = document.getElementById('open-panel-btn');

let previousView = null;

// Set today's date implicitly, restrict past dates and limit future to 15 days
const today = new Date();
const year = today.getFullYear();
const month = String(today.getMonth() + 1).padStart(2, '0');
const day = String(today.getDate()).padStart(2, '0');
const formattedToday = `${year}-${month}-${day}`;

// Calculate maximum allowed date: 15 days from today
const maxDateObj = new Date(today);
maxDateObj.setDate(maxDateObj.getDate() + 15);
const maxYear = maxDateObj.getFullYear();
const maxMonth = String(maxDateObj.getMonth() + 1).padStart(2, '0');
const maxDay = String(maxDateObj.getDate()).padStart(2, '0');
const formattedMaxDate = `${maxYear}-${maxMonth}-${maxDay}`;

dateInput.value = formattedToday;
dateInput.min = formattedToday;
dateInput.max = formattedMaxDate;

// Ensure default value does not exceed the max
if (dateInput.value > formattedMaxDate) {
    dateInput.value = formattedMaxDate;
}

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

    if (appMode === 'history') {
        const features = map.queryRenderedFeatures(e.point, {
            layers: ['unclustered-point', 'clusters']
        });

        if (!features.length) {
            closeHistoryPanel();
            return;
        }

        const feature = features[0];

        if (feature.layer.id === 'clusters') {
            const clusterId = feature.properties.cluster_id;
            map.getSource('historical-fires').getClusterExpansionZoom(
                clusterId,
                (err, zoom) => {
                    if (err) return;
                    map.easeTo({
                        center: feature.geometry.coordinates,
                        zoom: zoom + 1
                    });
                }
            );
        } else if (feature.layer.id === 'unclustered-point') {
            const props = feature.properties;
            saveCurrentView();
            openHistoryPanel(lng, lat, props);
            map.flyTo({
                center: [lng, lat],
                zoom: 14,
                pitch: 45,
                duration: 1500,
                essential: true
            });
        }
        return; // Detener flujo para modo historia
    }

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
    closeHistoryPanel(); // Cerrar el otro si está abierto
    panel.classList.remove('is-hidden');
    openPanelBtn.classList.add('hidden');
}

function closePanel() {
    panel.classList.add('is-hidden');
    openPanelBtn.classList.remove('hidden');
}

function openHistoryPanel(lng, lat, data) {
    closePanel(); // Cerrar el predictivo si está abierto
    historyPanel.classList.remove('is-hidden');
    openPanelBtn.classList.add('hidden');

    const latDir = lat >= 0 ? 'N' : 'S';
    const lngDir = lng >= 0 ? 'E' : 'W';
    historyHeaderCoords.textContent = `${Math.abs(lat).toFixed(4)}° ${latDir}, ${Math.abs(lng).toFixed(4)}° ${lngDir}`;

    historyContent.innerHTML = `
        <div class="data-section-title">Detalles Atmosféricos</div>
        <div class="data-grid">
            <div class="data-item full-width">
                <div class="data-label">Fecha de Registro</div>
                <div class="data-value">${data.fecha || 'N/A'}</div>
            </div>
            <div class="data-item">
                <div class="data-label">Temp Med (${data.temp_min} - ${data.temp_max})</div>
                <div class="data-value highlight">${data.temp_mean} °C</div>
            </div>
            <div class="data-item">
                <div class="data-label">Humedad Med</div>
                <div class="data-value highlight">${data.humidity_mean} %</div>
            </div>
            <div class="data-item">
                <div class="data-label">Viento Max (Ráfagas)</div>
                <div class="data-value">${data.wind_speed_max} km/h (${data.wind_gusts_max})</div>
            </div>
            <div class="data-item">
                <div class="data-label">Precipitación</div>
                <div class="data-value">${data.precipitation} mm</div>
            </div>
            <div class="data-item">
                <div class="data-label">Cobertura Nubosa</div>
                <div class="data-value">${data.cloud_cover} %</div>
            </div>
            <div class="data-item">
                <div class="data-label">Temp del Suelo</div>
                <div class="data-value">${data.soil_temp} °C</div>
            </div>
        </div>

        <div class="data-section-title">Terreno</div>
        <div class="data-grid">
            <div class="data-item">
                <div class="data-label">Elevación Centro</div>
                <div class="data-value">${data.elevacion_centro} m</div>
            </div>
            <div class="data-item">
                <div class="data-label">NDVI</div>
                <div class="data-value">${data.NDVI}</div>
            </div>
        </div>
    `;
}

function closeHistoryPanel() {
    historyPanel.classList.add('is-hidden');
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

closeHistoryBtn.addEventListener('click', () => {
    closeHistoryPanel();
});

// Heatmap Toggle
heatmapToggle.addEventListener('change', (e) => {
    if (map.getLayer('fires-heatmap')) {
        map.setLayoutProperty('fires-heatmap', 'visibility', e.target.checked ? 'visible' : 'none');
    }
});

// Mode Toggle Event Listeners
btnModePrediction.addEventListener('click', () => {
    if (appMode === 'prediction') return;
    appMode = 'prediction';
    btnModePrediction.classList.add('active');
    btnModeHistory.classList.remove('active');
    heatmapToggleWrapper.classList.add('hidden');
    
    // Ocultar capas del histórico
    map.setLayoutProperty('fires-heatmap', 'visibility', 'none');
    map.setLayoutProperty('clusters', 'visibility', 'none');
    map.setLayoutProperty('cluster-count', 'visibility', 'none');
    map.setLayoutProperty('unclustered-point', 'visibility', 'none');
    
    map.getCanvas().style.cursor = '';
    closeHistoryPanel();
    
    if (currentMarker) currentMarker.addTo(map);
});

btnModeHistory.addEventListener('click', () => {
    if (appMode === 'history') return;
    appMode = 'history';
    btnModeHistory.classList.add('active');
    btnModePrediction.classList.remove('active');
    heatmapToggleWrapper.classList.remove('hidden');
    
    // Mostrar capas del histórico
    const showHeatmap = heatmapToggle.checked ? 'visible' : 'none';
    map.setLayoutProperty('fires-heatmap', 'visibility', showHeatmap);
    map.setLayoutProperty('clusters', 'visibility', 'visible');
    map.setLayoutProperty('cluster-count', 'visibility', 'visible');
    map.setLayoutProperty('unclustered-point', 'visibility', 'visible');
    
    map.getCanvas().style.cursor = 'pointer';
    closePanel();
    
    if (currentMarker) currentMarker.remove();
});

// Cursors per history point hovering
map.on('mouseenter', 'clusters', () => {
    if (appMode === 'history') map.getCanvas().style.cursor = 'pointer';
});
map.on('mouseleave', 'clusters', () => {
    if (appMode === 'history') map.getCanvas().style.cursor = 'pointer';
});
map.on('mouseenter', 'unclustered-point', () => {
    if (appMode === 'history') map.getCanvas().style.cursor = 'pointer';
});
map.on('mouseleave', 'unclustered-point', () => {
    if (appMode === 'history') map.getCanvas().style.cursor = 'pointer';
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
