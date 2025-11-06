from langchain.chat_models import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from dotenv import load_dotenv

load_dotenv()

PROFILE_PROMPT_TEMPLATE = """
You are an expert at writing concise, professional profiles for venture capital investment databases.

Firm Name: {vc_name}
Validated VC Investment Fields (JSON):
{fields_json}

Instructions:
- Write a short, factual, professional profile of the firm in 2-4 sentences for an investment directory.
- Include only the information from the validated fields.
- If a field is empty or null, do not mention it.
- Use third-person neutral voice.
- Highlight modality focus, disease/indication focus, geography, check size, and investment stage as appropriate.
- Do not add information that is not present in the input.
"""

profile_prompt = PromptTemplate(
    template=PROFILE_PROMPT_TEMPLATE,
    input_variables=["fields_json", "vc_name"]
)

llm_profile = ChatOpenAI(model="gpt-4.1-2025-04-14", temperature=0)
profile_chain = LLMChain(llm=llm_profile, prompt=profile_prompt)

def generate_vc_profile_summary(validated_fields, vc_name=None):
    import json
    try:
        fields_json = json.dumps(validated_fields)
    except Exception:
        fields_json = str(validated_fields)
    print("📝 Generating VC profile with:", fields_json)

    if vc_name:
        prefix = f"{vc_name} — "
    else:
        prefix = ""

    result = profile_chain.invoke({"fields_json": fields_json, "vc_name": vc_name})
    profile_text = result["text"].strip()
    print("📝 Profile output:", profile_text)
    return prefix + profile_text

# Example usage:
# validated_fields = {
#     "modality_focus": ["small molecule", "gene therapy"],
#     "disease_focus": ["oncology", "rare disease"],
#     "investment_check_size": "$10-25 million",
#     "geography": ["US", "Europe"],
#     "investment_stage": ["early-stage", "series A"]
# }
# print(generate_vc_profile_summary(validated_fields, "Test Ventures"))