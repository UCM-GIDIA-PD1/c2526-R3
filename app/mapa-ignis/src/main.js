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

    // Añadir fuente de datos de incendios históricos (sin clustering para heatmap)
    if (!map.getSource('historical-fires-heatmap')) {
        map.addSource('historical-fires-heatmap', {
            type: 'geojson',
            data: '/fires.geojson',
            cluster: false
        });
    }

    // Fuente con clustering para marcadores
    if (!map.getSource('historical-fires')) {
        map.addSource('historical-fires', {
            type: 'geojson',
            data: '/fires.geojson',
            cluster: true,
            clusterMaxZoom: 14,
            clusterRadius: 50
        });
    }

    const historyVisibility = appMode === 'history' ? 'visible' : 'none';

    // Heatmap layer using unclustered source for better gradient coverage
    if (!map.getLayer('fires-heatmap')) {
        map.addLayer({
            id: 'fires-heatmap',
            type: 'heatmap',
            source: 'historical-fires-heatmap',
            paint: {
                'heatmap-weight': 1,
                'heatmap-intensity': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, 0.6,
                    5, 1.2,
                    10, 2,
                    15, 3
                ],
                // Yellow → Orange → Red gradient matching the reference image
                'heatmap-color': [
                    'interpolate',
                    ['linear'],
                    ['heatmap-density'],
                    0, 'rgba(255, 255, 0, 0)',
                    0.15, 'rgba(255, 255, 100, 0.4)',
                    0.3, 'rgba(255, 230, 50, 0.55)',
                    0.5, 'rgba(255, 190, 0, 0.65)',
                    0.7, 'rgba(255, 140, 0, 0.75)',
                    0.85, 'rgba(255, 80, 0, 0.85)',
                    1, 'rgba(255, 20, 0, 0.95)'
                ],
                'heatmap-radius': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, 8,
                    3, 20,
                    5, 30,
                    7, 40,
                    9, 50,
                    12, 60
                ],
                'heatmap-opacity': [
                    'interpolate',
                    ['linear'],
                    ['zoom'],
                    0, 0.75,
                    9, 0.65,
                    12, 0.5
                ]
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
                    'rgba(181, 226, 140, 0.9)', 10, 
                    'rgba(241, 211, 87, 0.9)', 100, 
                    'rgba(253, 156, 115, 0.9)'
                ],
                'circle-radius': [
                    'step',
                    ['get', 'point_count'],
                    16, 10,
                    20, 100,
                    24
                ],
                'circle-stroke-width': 10,
                'circle-stroke-color': [
                    'step',
                    ['get', 'point_count'],
                    'rgba(110, 204, 57, 0.5)', 10,
                    'rgba(240, 194, 12, 0.5)', 100,
                    'rgba(241, 128, 23, 0.5)'
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
let selectedLat = null;
let selectedLon = null;
const API_BASE_URL = window.location.origin;

// Application Mode: 'prediction' | 'history'
let appMode = 'prediction';

// Mode DOM Elements
const btnModePrediction = document.getElementById('mode-prediction-btn');
const btnModeHistory = document.getElementById('mode-history-btn');
const layerControlWrapper = document.getElementById('layer-control-wrapper');
const layerIncendios = document.getElementById('layer-incendios');
const layerHeatmap = document.getElementById('layer-heatmap');

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
    if (appMode === 'prediction') {
        openPanelBtn.classList.remove('hidden');
    }
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
    // Button is only visible if we return to prediction mode, but here we stay in history mode
    // so we don't show any button to reopen history since history opens via marker click
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
    selectedLat = lat;
    selectedLon = lng;
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

// Layer Control: toggle incendios (clusters) layer
layerIncendios.addEventListener('change', (e) => {
    const vis = e.target.checked ? 'visible' : 'none';
    if (map.getLayer('clusters')) map.setLayoutProperty('clusters', 'visibility', vis);
    if (map.getLayer('cluster-count')) map.setLayoutProperty('cluster-count', 'visibility', vis);
    if (map.getLayer('unclustered-point')) map.setLayoutProperty('unclustered-point', 'visibility', vis);
});

// Layer Control: toggle heatmap layer
layerHeatmap.addEventListener('change', (e) => {
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
    layerControlWrapper.classList.add('hidden');
    
    // Ocultar capas del histórico
    map.setLayoutProperty('fires-heatmap', 'visibility', 'none');
    map.setLayoutProperty('clusters', 'visibility', 'none');
    map.setLayoutProperty('cluster-count', 'visibility', 'none');
    map.setLayoutProperty('unclustered-point', 'visibility', 'none');
    
    map.getCanvas().style.cursor = '';
    closeHistoryPanel();
    
    if (panel.classList.contains('is-hidden')) {
        openPanelBtn.classList.remove('hidden');
    }
    
    if (currentMarker) currentMarker.addTo(map);
});

btnModeHistory.addEventListener('click', () => {
    if (appMode === 'history') return;
    appMode = 'history';
    btnModeHistory.classList.add('active');
    btnModePrediction.classList.remove('active');
    layerControlWrapper.classList.remove('hidden');
    
    // Mostrar capas del histórico según checkboxes
    const showHeatmap = layerHeatmap.checked ? 'visible' : 'none';
    const showIncendios = layerIncendios.checked ? 'visible' : 'none';
    map.setLayoutProperty('fires-heatmap', 'visibility', showHeatmap);
    map.setLayoutProperty('clusters', 'visibility', showIncendios);
    map.setLayoutProperty('cluster-count', 'visibility', showIncendios);
    map.setLayoutProperty('unclustered-point', 'visibility', showIncendios);
    
    map.getCanvas().style.cursor = 'pointer';
    closePanel();
    openPanelBtn.classList.add('hidden');
    
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

actionBtn.addEventListener('click', async () => {
    if(!selectedLat || !selectedLon) {
        alert("Por favor, selecciona una ubicación en el mapa primero.");
        return;
    }
    
    actionBtn.disabled = true;
    actionBtn.textContent = 'Procesando...';
    
    try {
        await performPrediction();
        hasPrediction = true;
    } catch (error) {
        console.error("Prediction error:", error);
        alert("Error al realizar la predicción: " + error.message);
    } finally {
        actionBtn.disabled = false;
        updateUIState();
    }
});

async function performPrediction() {
    const endpoint = activeTab === 'riesgo' ? '/predict/ocurrencia' : '/predict/intensidad';
    const response = await fetch(`${API_BASE_URL}${endpoint}`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify({
            latitud: selectedLat,
            longitud: selectedLon,
            fecha: dateInput.value
        })
    });

    if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Error en el servidor");
    }

    const data = await response.json();
    renderPredictionResults(data);
}

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

function renderPredictionResults(data) {
    if (activeTab === 'riesgo') {
        const prob = (data.probabilidad * 100).toFixed(1);
        const riskClass = data.ocurrencia ? 'risk-high' : 'risk-low';
        
        let variablesHtml = '';
        if (data.variables_clave) {
            variablesHtml = `
                <div class="factors-title">Variables Clave en el Punto</div>
                <div class="key-variables-grid">
                    ${Object.entries(data.variables_clave).map(([name, val]) => `
                        <div class="key-variable-item">
                            <span class="var-name">${name}</span>
                            <span class="var-value">${val}</span>
                        </div>
                    `).join('')}
                </div>
            `;
        }

        let importanciasHtml = '';
        if (data.importancias && Object.keys(data.importancias).length > 0) {
            const maxImp = Math.max(...Object.values(data.importancias));
            importanciasHtml = `
                <div class="factors-title">Contribución al Riesgo (Importancia)</div>
                <div class="importance-chart">
                    ${Object.entries(data.importancias).map(([name, imp]) => {
                        const percent = (imp / maxImp * 100).toFixed(0);
                        return `
                            <div class="importance-item">
                                <div class="importance-label">
                                    <span>${name}</span>
                                    <span>${(imp * 100).toFixed(1)}%</span>
                                </div>
                                <div class="importance-bar-bg">
                                    <div class="importance-bar" style="width: ${percent}%"></div>
                                </div>
                            </div>
                        `;
                    }).join('')}
                </div>
            `;
        }
        
        resultsContainer.innerHTML = `
            <div class="result-section">
                <div class="result-header">Resultado de la Predicción</div>
                <div class="risk-probability ${riskClass}">
                    ${data.ocurrencia ? 'ALTO RIESGO' : 'RIESGO BAJO'}: ${prob}%
                </div>
                
                ${data.error ? `<div class="error-msg" style="color: #ef4444; margin-top: 10px;">${data.error}</div>` : ''}
                ${data.nota_informativa ? `<div class="note-msg" style="color: #f59e0b; font-size: 0.8rem; margin-top: 10px;">⚠️ ${data.nota_informativa}</div>` : ''}

                ${variablesHtml}
                ${importanciasHtml}

                <div class="factor-item" style="margin-top: 15px; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 10px;">
                    <div class="factor-header" style="font-size: 0.7rem; opacity: 0.6;">
                        Fecha: ${data.fecha_procesada} | Modelo: ${data.modelo_version}
                    </div>
                </div>
            </div>
        `;
    } else {
        const intensity = data.intensidad.toFixed(2);
        
        let variablesHtml = '';
        if (data.variables_clave) {
            variablesHtml = `
                <div class="factors-title">Condiciones en el Punto</div>
                <div class="key-variables-grid">
                    ${Object.entries(data.variables_clave).map(([name, val]) => `
                        <div class="key-variable-item">
                            <span class="var-name">${name}</span>
                            <span class="var-value">${val}</span>
                        </div>
                    `).join('')}
                </div>
            `;
        }

        resultsContainer.innerHTML = `
            <div class="result-section">
                <div class="result-header">Resultado de la Predicción de FRP</div>
                <div class="frp-title">FRP ESTIMADO:</div>
                <div class="frp-value">${intensity} MW</div>
                <div class="frp-subtitle">Potencia Radiativa del Fuego Estimada</div>
                
                ${data.error ? `<div class="error-msg" style="color: #ef4444; margin-top: 10px;">${data.error}</div>` : ''}
                ${data.nota_informativa ? `<div class="note-msg" style="color: #f59e0b; font-size: 0.8rem; margin-top: 10px;">⚠️ ${data.nota_informativa}</div>` : ''}
                
                ${variablesHtml}

                <div style="margin-top: 15px; font-size: 0.7rem; opacity: 0.6; border-top: 1px solid rgba(255,255,255,0.05); padding-top: 10px;">
                    Fecha: ${data.fecha_procesada} | Versión: ${data.modelo_version}
                </div>
            </div>
        `;
    }
}


coordsInput.addEventListener('change', (e) => {
    const val = e.target.value.trim();
    if (!val) return;

    // Check for "Lat, Lng" or similar formats, including degree symbols and N/S/E/W
    const regex = /([-+]?\d*\.?\d+)\s*°?\s*([NSns]?)[,\s]+([-+]?\d*\.?\d+)\s*°?\s*([EWew]?)/;
    const match = val.match(regex);

    if (match) {
        let lat = parseFloat(match[1]);
        const latDir = match[2].toUpperCase();
        let lng = parseFloat(match[3]);
        const lngDir = match[4].toUpperCase();

        if (latDir === 'S') lat = -Math.abs(lat);
        if (lngDir === 'W') lng = -Math.abs(lng);
        if (latDir === 'N') lat = Math.abs(lat);
        if (lngDir === 'E') lng = Math.abs(lng);
        
        // Basic check for Europe bounds
        if (lat < 27.6 || lat > 71.2 || lng < -31.8 || lng > 45.0) {
            alert("Atención: Las coordenadas introducidas parecen estar fuera de Europa.");
        }

        setCoordinates(lng, lat);
        saveCurrentView();
        
        map.flyTo({
            center: [lng, lat],
            zoom: 14,
            pitch: 65,
            duration: 1500,
            essential: true
        });
    } else {
        alert("Formato no válido. Usa un formato como '40.4168, -3.7838' o '40.41 N, 3.78 W'.");
        coordsInput.value = headerCoords.textContent;
    }
});

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

// Inicializar la vista abierta por defecto
map.once('style.load', () => {
    openPanel();
    setCoordinates(defaultCenter[0], defaultCenter[1]);
});

