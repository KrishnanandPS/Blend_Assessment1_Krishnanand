import pandas as pd
import os
from datetime import datetime

print("=" * 70)
print("GENAI URBAN MOBILITY INSIGHTS ASSISTANT")
print("=" * 70)

# Load KPI data for context
print("\n📊 Loading KPI data for context...")
df = pd.read_parquet('cleaned_trips.parquet')

# Create data summary
summary_stats = {
    'total_trips': len(df),
    'total_revenue': df['total_amount'].sum(),
    'avg_fare': df['fare_amount'].mean(),
    'avg_distance': df['trip_distance'].mean(),
    'avg_tip_pct': df['tip_percentage'].mean(),
    'peak_trips': df[df['is_peak']].shape[0],
    'weekend_trips': df[df['is_weekend']].shape[0]
}

# Load hourly breakdown
hourly_data = df.groupby('hour').agg({
    'fare_amount': 'count',
    'total_amount': 'sum'
}).rename(columns={'fare_amount': 'trips', 'total_amount': 'revenue'})

# Top 5 busiest hours
top_hours = hourly_data.nlargest(5, 'trips')

# Payment type analysis
payment_analysis = df.groupby('payment_type').agg({
    'fare_amount': 'count',
    'tip_percentage': 'mean'
}).rename(columns={'fare_amount': 'trips', 'tip_percentage': 'avg_tip_pct'})

print("✓ Data loaded successfully")
print(f"  • Total trips: {summary_stats['total_trips']:,}")
print(f"  • Total revenue: ${summary_stats['total_revenue']:,.2f}")

# Create comprehensive context
data_context = f"""
NYC Yellow Taxi Data - January 2015 Analysis

OVERALL METRICS:
- Total Trips: {summary_stats['total_trips']:,}
- Total Revenue: ${summary_stats['total_revenue']:,.2f}
- Average Fare: ${summary_stats['avg_fare']:.2f}
- Average Distance: {summary_stats['avg_distance']:.2f} miles
- Average Tip %: {summary_stats['avg_tip_pct']:.2f}%
- Peak Hour Trips: {summary_stats['peak_trips']:,} ({summary_stats['peak_trips'] / summary_stats['total_trips'] * 100:.1f}%)
- Weekend Trips: {summary_stats['weekend_trips']:,} ({summary_stats['weekend_trips'] / summary_stats['total_trips'] * 100:.1f}%)

TOP 5 BUSIEST HOURS:
{top_hours.to_string()}

PAYMENT TYPE BREAKDOWN:
{payment_analysis.to_string()}
"""

print("\n" + "=" * 70)
print("INITIALIZING GENAI MODEL")
print("=" * 70)

print("\n🤖 Using Groq API (Ultra-fast inference)")

try:
    from groq import Groq

    # Configure with API key
    client = Groq(api_key='ENTER KEY HERE')


    def ask_question(question):
        prompt = f"Context: {data_context}\n\nQuestion: {question}\n\nProvide data-driven insights with specific numbers from the context above."

        response = client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system",
                 "content": "You are an expert urban mobility analyst. Provide concise, data-driven insights."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.3,
            max_tokens=500
        )
        return response.choices[0].message.content


    print("✓ Groq API initialized successfully")

except Exception as e:
    print(f"❌ Error: {e}")
    print("\nMake sure to install: pip install groq")
    exit()

print("\n" + "=" * 70)
print("TESTING GENAI ASSISTANT WITH SAMPLE QUESTIONS")
print("=" * 70)

# Predefined questions
questions = [
    "What were the top 3 busiest hours in January 2015 and why do you think demand was high during those times?",
    "When should taxi drivers work to maximize their earnings based on this data?",
    "How does tip percentage vary throughout the day? What insights can you provide?",
    "What patterns do you see in weekend vs weekday demand?",
    "Generate a 3-sentence executive summary of January 2015 taxi operations for city planners."
]

# Store Q&A pairs
qa_results = []

for i, question in enumerate(questions, 1):
    print(f"\n{'=' * 70}")
    print(f"QUESTION {i}")
    print('=' * 70)
    print(f"❓ {question}")
    print(f"\n💡 ANSWER:")
    print("-" * 70)

    try:
        answer = ask_question(question)
        print(answer)

        qa_results.append({
            'question': question,
            'answer': answer,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

    except Exception as e:
        print(f"❌ Error: {e}")
        qa_results.append({
            'question': question,
            'answer': f"Error: {e}",
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })

print("\n" + "=" * 70)
print("SAVING RESULTS")
print("=" * 70)

# Save Q&A to CSV
qa_df = pd.DataFrame(qa_results)
qa_df.to_csv('genai_qa_results.csv', index=False)
print("✓ Saved: genai_qa_results.csv")

# Save formatted text output
with open('genai_insights.txt', 'w', encoding='utf-8') as f:
    f.write("=" * 70 + "\n")
    f.write("NYC TAXI GENAI INSIGHTS - JANUARY 2015\n")
    f.write("=" * 70 + "\n\n")

    for i, qa in enumerate(qa_results, 1):
        f.write(f"QUESTION {i}:\n")
        f.write(f"{qa['question']}\n\n")
        f.write(f"ANSWER:\n")
        f.write(f"{qa['answer']}\n\n")
        f.write("-" * 70 + "\n\n")

print("✓ Saved: genai_insights.txt")

print("\n" + "=" * 70)
print("SCALABILITY FOR 100GB+ DATA")
print("=" * 70)

scalability_doc = """
📈 GENAI SCALABILITY STRATEGY FOR 100GB+ DATASETS

1. DATA PREPARATION:
   Current: Load full 12.6M rows into memory
   Scale: Pre-aggregate to summary tables (hourly, daily, zone-level)
   Result: 12.6M rows → 10K summary rows (1000x smaller context)

2. VECTOR DATABASE (RAG APPROACH):
   Tool: FAISS or Pinecone
   Process:
   - Chunk KPIs into logical segments (hourly stats, zone stats, etc.)
   - Generate embeddings for each chunk
   - Store in vector DB with metadata
   - Query: User asks → Find relevant chunks → Send to LLM
   Benefit: Only send relevant data (not entire 100GB)

3. RETRIEVAL AUGMENTED GENERATION (RAG):
   Architecture:
   User Question → Embedding Model → Vector Search → Top 5 relevant chunks
   → Combine with question → LLM → Answer with citations

   Example:
   Q: "What's the busiest zone?"
   → Retrieve only zone-level aggregations (not all 100GB)
   → LLM answers using 5KB of relevant data

4. QUERY OPTIMIZATION:
   - Cache frequent queries (Redis)
   - Pre-compute common insights
   - Use LLM for natural language → SQL translation
   - Execute SQL on data warehouse, not LLM

5. COST & PERFORMANCE:
   Current: FREE (Groq API with ultra-fast inference)
   Speed: ~500 tokens/second (10x faster than OpenAI)
   Scale: Groq for production with rate limits
   Alternative: Azure OpenAI for enterprise

6. PRODUCTION ARCHITECTURE:
   User → FastAPI → Vector DB (FAISS) → Azure Blob (Parquet) → Groq API
   - FastAPI: REST API for questions
   - FAISS: Fast vector similarity search
   - Azure Blob: Stores pre-aggregated Parquet files
   - Groq: Ultra-fast LLM inference with Llama 3.3

IMPLEMENTATION EXAMPLE:
```python
from langchain.vectorstores import FAISS
from langchain.embeddings import HuggingFaceEmbeddings
from groq import Groq

# Create embeddings for KPI summaries
embeddings = HuggingFaceEmbeddings()
vectorstore = FAISS.from_texts(kpi_summaries, embeddings)

# Query with RAG
relevant_docs = vectorstore.similarity_search(user_question, k=5)
context = "\\n".join([doc.page_content for doc in relevant_docs])

# Send to Groq with relevant context only
client = Groq(api_key="your-key")
response = client.chat.completions.create(
    model="llama-3.3-70b-versatile",
    messages=[{"role": "user", "content": context + user_question}]
)"""

