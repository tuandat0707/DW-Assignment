# Flight Delay Prediction Streamlit App

This is a production-grade analytics dashboard and prediction tool for US flight delays.

## 🚀 How to Run

1. **Install Dependencies**:
   ```bash
   pip install -r streamlit_app/requirements.txt
   ```

2. **Database Setup**:
   Ensure your SQL Server Docker container (`flight-dw-sqlserver`) is running on port `1433`.

3. **Launch the App**:
   ```bash
   cd streamlit_app
   streamlit run app.py
   ```

## 📂 Project Structure
- `app.py`: Main entry point and sidebar configuration.
- `pages/`: Individual analytical tools (EDA, Predictor, Performance, Explorer).
- `utils/`: Reusable database and model utilities.
- `models/`: Should contain `xgboost_delay_model.pkl`, `label_encoders.pkl`, and optionally `eval_results.pkl`.

## 🛠️ Model Asset Generation

To ensure the **Predictor** and **Performance** pages work correctly, you must export your training assets from your notebook:

```python
import joblib

# Export LabelEncoders (after fitting)
# encoders_dict should be { 'column_name': LabelEncoderObject }
joblib.dump(encoders_dict, "models/label_encoders.pkl")

# Export Model
joblib.dump(model, "models/xgboost_delay_model.pkl")

# Export Evaluation Results (Optional but recommended for Page 3)
eval_data = {
    "report": classification_report(y_test, y_pred, output_dict=True),
    "cm": confusion_matrix(y_test, y_pred),
    "fpr": fpr,
    "tpr": tpr,
    "auc": auc_score
}
joblib.dump(eval_data, "models/eval_results.pkl")
```

## 🔴 Troubleshooting
- **Database Status Red**: Check if Docker is running and if the ODBC driver for SQL Server is installed on your machine.
- **Model Not Found**: Check if the `.pkl` files exist in the `models/` directory. The app expects `label_encoders.pkl` and `xgboost_delay_model.pkl`.
