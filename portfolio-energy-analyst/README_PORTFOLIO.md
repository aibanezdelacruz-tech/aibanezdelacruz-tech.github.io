# Energy Portfolio — Alejandro Ibáñez de la Cruz

Portfolio profesional specializado en análisis de mercados energéticos, forecasting de precios OMIE y visualización de datos del sector eléctrico.

## 🌐 Demo en Vivo

**Portal principal**: [aibanezdelacruz.github.io](https://aibanezdelacruz.github.io)

- Perfil profesional con visión técnico-transversal
- Proyecto OMIE Spot Price Analysis con gráficos interactivos
- Análisis del mercado eléctrico español e indicadores clave
- Links a repositorios técnicos y recursos

## 📊 Proyecto Principal: OMIE Spot Price Forecasting

**Pipeline completo** de análisis y predicción de precios horarios del mercado diario español (2023–2025):

- **Datos**: 18,920+ horas OMIE reales + contexto ESIOS/REE
- **Modelo**: XGBoost con 30+ features de ingeniería
- **Performance**: MAE 7.91 €/MWh | RMSE 11.86 €/MWh | 61% mejora vs Baseline
- **Focus**: Solar peak shaving, precios negativos, merit order
- **Visualizaciones**: Heatmap de precios intradiarios + historical timeseries

**Repositorio técnico**: [omie-spot-analysis](https://github.com/aibanezdelacruz-tech/omie-spot-analysis)

## 🛠️ Stack Técnico

```
Languages:  Python · SQL · JavaScript · R
ML:         XGBoost · Scikit-learn · Pandas · NumPy
Energy:     OMIE API · ESIOS (REE) · MIBEL · ENTSO-E
Viz:        Plotly.js · Matplotlib · Seaborn
Deploy:     Git · GitHub Pages · Jupyter Notebooks
```

## 📁 Estructura del Proyecto

```
energy-portfolio/
├── index.html                  # Portal main (single-page app)
├── assets/
│   └── data/
│       ├── heatmap_data.json   # Matriz 24h × días (precios)
│       ├── timeseries_data.json # Histórico diario + media móvil
│       ├── stats.json          # KPIs agregados
│       ├── metrics.json        # Métricas modelo ML
│       ├── monthly_prices.json # Precios agregados
│       └── predictions.json    # Predicciones vs real
└── README.md                   # Este archivo
```

## 🚀 Deployment

Este portfolio se despliega automáticamente a **GitHub Pages** desde el repositorio `aibanezdelacruz.github.io`:

```bash
# Cualquier push a main/ dispara el deployment
git add .
git commit -m "Update portfolio"
git push origin main
```

La página estará disponible en: `https://aibanezdelacruz.github.io`

## 🎯 Características

✅ **Design Dark-Mode** con paleta energética (Cyan → Verde → Naranja)  
✅ **Visualizaciones interactivas Plotly.js** con zoom, pan y hover  
✅ **Responsive en móvil** y desktop  
✅ **SEO básico** con meta tags optimizados  
✅ **Single Page App** con navegación smooth  

## 📞 Contacto & Links

- **Portfolio Web**: [aibanezdelacruz.github.io](https://aibanezdelacruz.github.io)
- **GitHub Técnico**: [github.com/aibanezdelacruz-tech](https://github.com/aibanezdelacruz-tech)
- **Proyecto OMIE**: [omie-spot-analysis](https://github.com/aibanezdelacruz-tech/omie-spot-analysis)

---

**Última actualización**: Marzo 2025  
**Tecnologías**: HTML5 · CSS3 · JavaScript (Vanila) · Plotly.js · GitHub Pages
