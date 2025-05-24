import openai
import os

def add_ai_insights_to_email(metrics_html, metrics):
    """Add AI-generated insights to email report"""
    if not os.getenv('OPENAI_API_KEY'):
        return metrics_html  # Gracefully skip if no API key
    
    try:
        openai.api_key = os.getenv('OPENAI_API_KEY')
        
        # Generate insights
        insights = generate_ai_insights(metrics)
        
        # Add to HTML
        insights_html = f"""
        <h3>AI-Generated Insights</h3>
        <div style="background-color: #f0f8ff; padding: 15px; border-radius: 5px;">
            {insights.replace('\n', '<br>')}
        </div>
        """
        
        return metrics_html + insights_html
    except Exception as e:
        logging.warning(f"Failed to generate AI insights: {e}")
        return metrics_html  # Return original on failure