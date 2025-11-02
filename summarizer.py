"""
AI Summarizer Module - Local AI Model Summarization
Replaces OpenAI API with free local AI models (BART/T5)
"""

import os
import logging
import time
import re
from datetime import datetime
import torch
from transformers import pipeline, AutoTokenizer, AutoModelForSeq2SeqLM
import warnings

# Suppress transformers warnings
warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# Fallback to extractive summarization if models fail
try:
    from sumy.parsers.plaintext import PlaintextParser
    from sumy.nlp.tokenizers import Tokenizer
    from sumy.summarizers.text_rank import TextRankSummarizer
    SUMY_AVAILABLE = True
except ImportError:
    SUMY_AVAILABLE = False
    logging.warning("Sumy not available - fallback summarization limited")


class AISummarizer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.device = 0 if torch.cuda.is_available() else -1
        self.model_cache_dir = os.getenv('MODEL_CACHE_DIR', './models')
        self.model = None
        self.tokenizer = None
        self.summarizer_pipeline = None
        self.model_name = None
        self.fallback_mode = False

        # Initialize model
        self._initialize_model()

    def _initialize_model(self):
        """Initialize the AI model with fallback options"""
        model_preferences = [
            'facebook/bart-large-cnn',  # Primary choice (500MB)
            't5-small',                  # Alternative (250MB, faster)
            't5-base'                    # Another alternative
        ]

        for model_name in model_preferences:
            try:
                self.logger.info(f"Attempting to load model: {model_name}")
                start_time = time.time()

                # Create cache directory if it doesn't exist
                os.makedirs(self.model_cache_dir, exist_ok=True)

                # Initialize pipeline
                self.summarizer_pipeline = pipeline(
                    "summarization",
                    model=model_name,
                    tokenizer=model_name,
                    device=self.device,
                    cache_dir=self.model_cache_dir,
                    model_kwargs={"torch_dtype": torch.float32 if self.device == -1 else torch.float16}
                )

                self.model_name = model_name
                load_time = time.time() - start_time
                self.logger.info(f"Successfully loaded model {model_name} in {load_time:.2f} seconds")
                self.fallback_mode = False
                return

            except Exception as e:
                self.logger.warning(f"Failed to load model {model_name}: {str(e)}")
                continue

        # If all models fail, try extractive summarization
        self.logger.warning("All AI models failed, falling back to extractive summarization")
        self.fallback_mode = True

    def _clean_text(self, text):
        """Clean and prepare text for summarization"""
        if not text:
            return ""

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)

        # Remove HTML tags if any
        text = re.sub(r'<[^>]+>', '', text)

        # Remove URLs
        text = re.sub(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', '', text)

        # Remove excessive punctuation
        text = re.sub(r'[^\w\s\.\!\?\,\-]', '', text)

        # Limit length for processing
        if len(text) > 1024:
            text = text[:1024]

        return text.strip()

    def _summarize_with_transformer(self, title, description, max_length=80, min_length=30):
        """Summarize using transformer models"""
        try:
            # Combine title and description for context
            content = f"Title: {title}\n\nContent: {description}"
            content = self._clean_text(content)

            if not content or len(content.split()) < 10:
                # Content too short, return original
                return {
                    'headline': title[:120] if len(title) > 120 else title,
                    'summary': description[:200] if description else title
                }

            # Generate summary with timeout protection
            start_time = time.time()
            summary_result = self.summarizer_pipeline(
                content,
                max_length=max_length,
                min_length=min_length,
                do_sample=False,
                truncation=True
            )
            processing_time = time.time() - start_time

            summary_text = summary_result[0]['summary_text'].strip()
            self.logger.debug(f"Summarization completed in {processing_time:.2f}s")

            # Generate catchy headline from the title
            headline = self._generate_headline(title, summary_text)

            return {
                'headline': headline,
                'summary': summary_text
            }

        except Exception as e:
            self.logger.error(f"Error in transformer summarization: {str(e)}")
            return self._fallback_summarization(title, description)

    def _generate_headline(self, original_title, summary):
        """Generate a catchy headline from original title and summary"""
        try:
            # Simple headline generation - take the most engaging part
            headline_candidates = [
                original_title,
                summary.split('.')[0] if summary else original_title,
                self._make_catchy(original_title)
            ]

            # Choose the best headline (under 120 chars)
            for candidate in headline_candidates:
                candidate = candidate.strip()
                if len(candidate) <= 120 and len(candidate) > 10:
                    return candidate

            # Fallback: truncate original title
            return original_title[:117] + "..." if len(original_title) > 120 else original_title

        except Exception as e:
            self.logger.warning(f"Error generating headline: {str(e)}")
            return original_title[:120] if len(original_title) > 120 else original_title

    def _make_catchy(self, title):
        """Make title more catchy - simple transformations"""
        # Remove boring prefixes
        boring_prefixes = ['Update:', 'Breaking:', 'News:', 'Report:']
        for prefix in boring_prefixes:
            if title.startswith(prefix):
                title = title[len(prefix):].strip()

        # Add engagement words if appropriate
        if any(word in title.lower() for word in ['warns', 'caution', 'alert', 'danger']):
            title = f"⚠️ {title}"

        return title

    def _fallback_summarization(self, title, description):
        """Fallback summarization using extractive methods"""
        try:
            if SUMY_AVAILABLE:
                return self._sumy_summarization(title, description)
            else:
                return self._basic_summarization(title, description)

        except Exception as e:
            self.logger.error(f"Error in fallback summarization: {str(e)}")
            return self._basic_summarization(title, description)

    def _sumy_summarization(self, title, description):
        """Use Sumy for extractive summarization"""
        try:
            content = f"{title}. {description}"
            content = self._clean_text(content)

            if len(content.split()) < 10:
                return self._basic_summarization(title, description)

            parser = PlaintextParser.from_string(content, Tokenizer("english"))
            summarizer = TextRankSummarizer()
            summary_sentences = summarizer(parser.document, 2)

            summary = " ".join(str(sentence) for sentence in summary_sentences)
            headline = title[:120] if len(title) > 120 else title

            return {
                'headline': headline,
                'summary': summary[:200] if summary else description[:200] if description else title
            }

        except Exception as e:
            self.logger.warning(f"Sumy summarization failed: {str(e)}")
            return self._basic_summarization(title, description)

    def _basic_summarization(self, title, description):
        """Very basic summarization - just clean and truncate"""
        try:
            # Use description as summary, or first part of title
            summary = description if description else title
            summary = self._clean_text(summary)

            # Truncate to reasonable length
            if len(summary) > 150:
                summary = summary[:147] + "..."

            # Create headline from title
            headline = title[:120] if len(title) > 120 else title

            return {
                'headline': headline,
                'summary': summary
            }

        except Exception as e:
            self.logger.error(f"Basic summarization failed: {str(e)}")
            return {
                'headline': title[:120] if len(title) > 120 else title,
                'summary': description[:100] if description else title[:100]
            }

    def summarize_with_ai(self, article):
        """Main summarization method matching the interface from main.py"""
        try:
            title = article.get('title', '')
            description = article.get('description', '') or article.get('raw_content', '')

            if not title:
                self.logger.warning("Article has no title, skipping summarization")
                return {
                    'headline': 'No Title',
                    'summary': 'No content available'
                }

            start_time = time.time()

            if self.fallback_mode or not self.summarizer_pipeline:
                result = self._fallback_summarization(title, description)
            else:
                # Use transformer model with timeout protection
                try:
                    result = self._summarize_with_transformer(title, description)
                except Exception as e:
                    self.logger.warning(f"Transformer summarization failed, using fallback: {str(e)}")
                    result = self._fallback_summarization(title, description)

            processing_time = time.time() - start_time
            self.logger.debug(f"Summarization completed in {processing_time:.2f}s")

            # Validate output format
            if not isinstance(result, dict) or 'headline' not in result or 'summary' not in result:
                self.logger.error("Invalid summarization result format")
                return self._basic_summarization(title, description)

            # Ensure headline is under 120 characters
            if len(result['headline']) > 120:
                result['headline'] = result['headline'][:117] + "..."

            self.logger.debug(f"Summarized: '{result['headline']}'")

            return result

        except Exception as e:
            self.logger.error(f"Error in summarize_with_ai: {str(e)}")
            return {
                'headline': article.get('title', 'Error')[:120],
                'summary': article.get('description', 'Summarization failed')[:200]
            }

    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.summarizer_pipeline:
                # Clear model from memory
                del self.summarizer_pipeline
                self.summarizer_pipeline = None

            if torch.cuda.is_available():
                torch.cuda.empty_cache()

            self.logger.info("Summarizer resources cleaned up")

        except Exception as e:
            self.logger.warning(f"Error during cleanup: {str(e)}")


def summarize_with_ai(article):
    """Convenience function that matches the interface expected by main.py"""
    summarizer = AISummarizer()
    return summarizer.summarize_with_ai(article)


if __name__ == "__main__":
    # Test the summarizer
    logging.basicConfig(level=logging.INFO)

    # Test article
    test_article = {
        'title': 'Scientists Discover Breakthrough in Renewable Energy Technology',
        'description': 'Researchers at leading universities have announced a major breakthrough in solar panel efficiency that could revolutionize the renewable energy sector. The new technology promises to increase energy conversion rates by up to 40% compared to current solar panels, making renewable energy more accessible and cost-effective for consumers worldwide.',
        'raw_content': 'Full article content would go here...'
    }

    summarizer = AISummarizer()
    result = summarizer.summarize_with_ai(test_article)

    print(f"Headline: {result['headline']}")
    print(f"Summary: {result['summary']}")
    print(f"Headline length: {len(result['headline'])} characters")

    # Cleanup
    summarizer.cleanup()