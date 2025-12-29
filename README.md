# 🏥 Healthcare Data Pipeline & Analytics

> Automated ETL pipeline processing NYS Health Data with interactive Tableau dashboards

![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white)
![AWS](https://img.shields.io/badge/AWS-232F3E?style=flat&logo=amazon-aws&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?style=flat&logo=tableau&logoColor=white)

## 📋 Project Overview

Developed automated ETL pipelines using Python and AWS stack to process New York State Health Data. Created interactive Tableau dashboards for medical students of Utica University for comprehensive trend analysis and data visualization.

## 🎯 Key Achievements

- ✅ **Automated ETL Pipeline** - Processing 100K+ health records automatically
- ✅ **AWS Infrastructure** - Scalable cloud-based data processing
- ✅ **Interactive Dashboards** - Tableau visualizations for 50+ medical students
- ✅ **Trend Analysis** - Historical and predictive health analytics
- ✅ **Time Savings** - Reduced manual processing by 20+ hours/week

## 🛠️ Technology Stack

| Category | Technologies |
|----------|-------------|
| **Languages** | Python 3.9+ |
| **Cloud** | AWS (S3, Lambda, Glue) |
| **Data Processing** | Pandas, NumPy |
| **Visualization** | Tableau |
| **Workflow** | Apache Airflow |

## 🏗️ Pipeline Architecture
```
NYS Health Database
        ↓
AWS Lambda (Extraction)
        ↓
AWS S3 (Raw Data Storage)
        ↓
AWS Glue (ETL Processing)
        ↓
AWS S3 (Processed Data)
        ↓
Tableau Server
        ↓
Interactive Dashboards
```

## 📊 Data Processed

- **Patient Demographics** - Age, gender, location distribution
- **Disease Prevalence** - Condition tracking across regions
- **Healthcare Utilization** - Hospital visits, procedures, admissions
- **Regional Trends** - Geographic health patterns and disparities
- **Seasonal Patterns** - Time-based health insights and forecasting

## 📁 Project Structure
```
healthcare-data-pipeline/
├── src/
│   ├── etl_pipeline.py       # Main ETL pipeline
│   ├── data_extraction.py    # Data extraction from NYS DB
│   ├── data_transformation.py # Data cleaning & processing
│   └── tableau_export.py     # Tableau data preparation
├── data/
│   ├── raw/                  # Raw health data
│   └── processed/            # Processed datasets
├── dashboards/
│   └── screenshots/          # Dashboard images
├── config/
│   └── aws_config.yaml       # AWS configuration
├── requirements.txt
└── README.md
```

## 🚀 Getting Started

### Prerequisites
```bash
Python 3.9+
AWS Account (S3, Lambda, Glue)
Tableau Desktop/Server
```

### Installation

1. Clone repository
```bash
git clone https://github.com/Devu4987/healthcare-data-pipeline.git
cd healthcare-data-pipeline
```

2. Install dependencies
```bash
pip install -r requirements.txt
```

3. Configure AWS credentials
```bash
aws configure
```

4. Run ETL pipeline
```bash
python src/etl_pipeline.py
```

## 💻 Usage Example
```python
from src.etl_pipeline import HealthcareETL

# Initialize pipeline
pipeline = HealthcareETL()

# Extract data from NYS database
pipeline.extract_data(start_date='2024-01-01', end_date='2024-12-31')

# Transform data
pipeline.transform_data()

# Load to S3
pipeline.load_to_s3(bucket='healthcare-data')

# Generate Tableau extract
pipeline.create_tableau_extract()
```

## 📈 Dashboard Features

### 1. Patient Demographics Overview
- Age distribution across regions
- Gender breakdown and trends
- Geographic population distribution
- Insurance coverage analysis

### 2. Disease Prevalence Tracking
- Top 10 health conditions
- Temporal trend analysis
- Regional disease comparisons
- Risk factor identification

### 3. Healthcare Utilization
- Hospital admission rates
- Emergency department visits
- Procedure frequency analysis
- Length of stay patterns

### 4. Regional Health Analysis
- County-level health metrics
- Urban vs rural health comparisons
- Healthcare access disparities
- Resource allocation insights

## 📊 Impact & Results

- 📉 **Automated** daily data processing (previously manual)
- 📈 **Processed** 100,000+ health records
- 👥 **Enabled** 50+ medical students to access real-time insights
- ⚡ **99% uptime** with automated monitoring
- 💾 **Reduced** storage costs by 40% through optimization

## 🔐 Security & Compliance

- **HIPAA-compliant** data handling procedures
- **Encrypted** data at rest and in transit
- **Access control** with AWS IAM roles
- **No PHI** (Protected Health Information) in visualizations
- **Regular security audits** and compliance checks
- **Audit logging** for all data access

## 🔄 ETL Process Details

### Extraction
- Connects to NYS Department of Health API
- Implements retry logic for failed requests
- Validates data integrity upon extraction
- Comprehensive activity logging

### Transformation
- Data cleaning and standardization
- Missing value imputation strategies
- Date/time normalization
- Categorical encoding
- Feature aggregation for analysis
- Multi-stage data quality checks

### Loading
- Optimized S3 bucket structure
- Date-based partitioning for efficient querying
- Metadata management and versioning
- Automatic backup creation

## 📊 Key Insights Delivered

- Identified seasonal flu patterns and outbreak predictions
- Tracked chronic disease prevalence across demographics
- Analyzed healthcare access disparities by region
- Monitored emergency department utilization trends
- Evaluated regional health outcomes and interventions

## 🛣️ Future Enhancements

- [ ] Real-time data streaming with AWS Kinesis
- [ ] Machine learning for predictive health analytics
- [ ] Mobile dashboard application
- [ ] Automated anomaly detection in health trends
- [ ] Integration with additional healthcare data sources
- [ ] Advanced NLP for clinical notes analysis

## 📄 License

MIT License - See [LICENSE](LICENSE) file for details

## 👤 Author

**Dev Narayan Chaudhary**
- 🎓 MBA in Business Analytics, Utica University (GPA: 3.95)
- 💼 Business Analyst Intern @ KCC Capital Partners
- 📧 sonusah98071@gmail.com
- 🔗 [LinkedIn](https://www.linkedin.com/in/dev-narayan-chaudhary-b68a292b3/)
- 💻 [GitHub](https://github.com/Devu4987)

## 🙏 Acknowledgments

- Utica University Medical Program
- NYS Department of Health for data access
- AWS for cloud infrastructure support
- Tableau Community for visualization best practices

---

⭐ **If you found this project helpful, please star the repository!**

💼 **Open to opportunities in Data Engineering, Healthcare Analytics, and Cloud Solutions**
