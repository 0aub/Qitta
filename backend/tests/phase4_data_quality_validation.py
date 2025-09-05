#!/usr/bin/env python3
"""
Phase 4.2: Data Quality Validation & Metrics
===========================================

Comprehensive data quality analysis including:
1. Field completeness analysis across all levels
2. Price accuracy and currency validation 
3. Review quality and metadata validation
4. Property detail accuracy verification
5. Quality scoring algorithm implementation
6. Manual validation comparison framework
"""

import requests
import json
import re
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from urllib.parse import urlparse

class DataQualityValidator:
    
    def __init__(self):
        self.base_url = "http://localhost:8004"
        self.validation_results = {}
        
    def analyze_field_completeness(self, properties: List[Dict], level: int) -> Dict[str, Any]:
        """Analyze field completeness for given extraction level."""
        if not properties:
            return {"error": "No properties to analyze"}
        
        # Define expected fields by level
        level_fields = {
            1: {
                "basic": ["title", "price_per_night", "airbnb_url"],
                "optional": ["rating", "image_url"]
            },
            2: {
                "basic": ["title", "price_per_night", "airbnb_url", "description", "amenities"],
                "enhanced": ["host_name", "bedrooms", "bathrooms", "latitude", "longitude"],
                "optional": ["host_avatar", "property_type", "address"]
            },
            3: {
                "basic": ["title", "price_per_night", "airbnb_url", "description", "amenities"],
                "enhanced": ["host_name", "bedrooms", "bathrooms"],
                "reviews": ["reviews", "reviews_count"],
                "optional": ["host_avatar", "property_type"]
            },
            4: {
                "basic": ["title", "price_per_night", "airbnb_url", "description", "amenities"],
                "enhanced": ["host_name", "bedrooms", "bathrooms"],
                "reviews": ["reviews", "reviews_count", "review_quality_score"],
                "comprehensive": ["reviewer_names", "review_dates", "review_ratings"],
                "optional": ["host_avatar", "property_type"]
            }
        }
        
        expected = level_fields.get(level, level_fields[2])
        completeness_analysis = {}
        
        # Analyze each field category
        for category, fields in expected.items():
            category_scores = []
            field_details = {}
            
            for field in fields:
                populated_count = sum(1 for prop in properties if self._is_field_populated(prop, field))
                completion_rate = populated_count / len(properties)
                category_scores.append(completion_rate)
                
                field_details[field] = {
                    "populated": populated_count,
                    "total": len(properties),
                    "completion_rate": completion_rate,
                    "quality_score": self._calculate_field_quality(properties, field)
                }
            
            completeness_analysis[category] = {
                "average_completion": sum(category_scores) / len(category_scores) if category_scores else 0,
                "field_details": field_details
            }
        
        # Overall completeness score
        all_scores = []
        for category_data in completeness_analysis.values():
            all_scores.append(category_data["average_completion"])
        
        overall_completeness = sum(all_scores) / len(all_scores) if all_scores else 0
        
        return {
            "level": level,
            "total_properties": len(properties),
            "overall_completeness": overall_completeness,
            "category_analysis": completeness_analysis,
            "quality_grade": self._get_quality_grade(overall_completeness)
        }
    
    def _is_field_populated(self, prop: Dict, field: str) -> bool:
        """Check if a field is properly populated."""
        value = prop.get(field)
        
        if value is None:
            return False
        
        if isinstance(value, str):
            return len(value.strip()) > 0 and value.strip().lower() not in ['n/a', 'null', 'none', '']
        
        if isinstance(value, list):
            return len(value) > 0 and any(item for item in value if item)
        
        if isinstance(value, (int, float)):
            return value > 0
        
        return bool(value)
    
    def _calculate_field_quality(self, properties: List[Dict], field: str) -> float:
        """Calculate quality score for a specific field."""
        populated_values = [prop.get(field) for prop in properties if self._is_field_populated(prop, field)]
        
        if not populated_values:
            return 0.0
        
        quality_score = 0.0
        total_checks = 0
        
        # Field-specific quality checks
        if field == "title":
            for title in populated_values:
                total_checks += 1
                if isinstance(title, str) and len(title) >= 10:
                    quality_score += 1
        
        elif field == "price_per_night":
            for price in populated_values:
                total_checks += 1
                if isinstance(price, (int, float)) and 10 <= price <= 10000:
                    quality_score += 1
        
        elif field == "description":
            for desc in populated_values:
                total_checks += 1
                if isinstance(desc, str) and len(desc) >= 50:
                    quality_score += 1
        
        elif field == "amenities":
            for amenities in populated_values:
                total_checks += 1
                if isinstance(amenities, list) and len(amenities) >= 3:
                    quality_score += 1
        
        elif field == "reviews":
            for reviews in populated_values:
                total_checks += 1
                if isinstance(reviews, list) and len(reviews) >= 1:
                    quality_score += 1
        
        elif field in ["latitude", "longitude"]:
            for coord in populated_values:
                total_checks += 1
                if isinstance(coord, (int, float)) and -180 <= coord <= 180:
                    quality_score += 1
        
        else:
            # Generic quality check
            for value in populated_values:
                total_checks += 1
                if value:  # Just check if populated
                    quality_score += 1
        
        return quality_score / total_checks if total_checks > 0 else 0.0
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to letter grade."""
        if score >= 0.95:
            return "A+"
        elif score >= 0.90:
            return "A"
        elif score >= 0.85:
            return "B+"
        elif score >= 0.80:
            return "B"
        elif score >= 0.75:
            return "C+"
        elif score >= 0.70:
            return "C"
        elif score >= 0.60:
            return "D"
        else:
            return "F"
    
    def validate_price_accuracy(self, properties: List[Dict]) -> Dict[str, Any]:
        """Validate price data accuracy and consistency."""
        price_analysis = {
            "total_properties": len(properties),
            "properties_with_prices": 0,
            "price_distribution": {},
            "currency_analysis": {},
            "price_quality_issues": []
        }
        
        prices = []
        currencies = {}
        
        for i, prop in enumerate(properties):
            price = prop.get("price_per_night")
            
            if price and isinstance(price, (int, float)) and price > 0:
                price_analysis["properties_with_prices"] += 1
                prices.append(price)
                
                # Currency detection
                currency = prop.get("currency", "USD")
                currencies[currency] = currencies.get(currency, 0) + 1
                
                # Price validation checks
                if price < 5:
                    price_analysis["price_quality_issues"].append(f"Property {i+1}: Suspiciously low price (${price})")
                elif price > 5000:
                    price_analysis["price_quality_issues"].append(f"Property {i+1}: Very high price (${price})")
        
        if prices:
            prices.sort()
            price_analysis["price_distribution"] = {
                "min_price": min(prices),
                "max_price": max(prices),
                "average_price": sum(prices) / len(prices),
                "median_price": prices[len(prices)//2],
                "price_range_reasonable": min(prices) >= 10 and max(prices) <= 2000
            }
        
        price_analysis["currency_analysis"] = currencies
        price_analysis["price_completion_rate"] = price_analysis["properties_with_prices"] / len(properties)
        price_analysis["price_quality_grade"] = self._get_quality_grade(price_analysis["price_completion_rate"])
        
        return price_analysis
    
    def validate_review_quality(self, properties: List[Dict]) -> Dict[str, Any]:
        """Validate review extraction quality and metadata."""
        review_analysis = {
            "total_properties": len(properties),
            "properties_with_reviews": 0,
            "review_statistics": {},
            "metadata_quality": {},
            "quality_issues": []
        }
        
        all_reviews = []
        review_counts = []
        metadata_scores = []
        
        for i, prop in enumerate(properties):
            reviews = prop.get("reviews", [])
            reviews_count = prop.get("reviews_count", 0)
            
            if reviews and isinstance(reviews, list) and len(reviews) > 0:
                review_analysis["properties_with_reviews"] += 1
                review_counts.append(len(reviews))
                all_reviews.extend(reviews)
                
                # Analyze review metadata quality
                metadata_score = 0
                total_metadata_checks = 0
                
                for review in reviews[:5]:  # Check first 5 reviews
                    total_metadata_checks += 5  # 5 metadata fields
                    
                    if review.get("text") and len(review.get("text", "")) > 20:
                        metadata_score += 1
                    if review.get("reviewer_name") and review.get("reviewer_name") != "Anonymous":
                        metadata_score += 1
                    if review.get("review_date"):
                        metadata_score += 1
                    if review.get("review_rating"):
                        metadata_score += 1
                    if review.get("reviewer_location"):
                        metadata_score += 1
                
                if total_metadata_checks > 0:
                    metadata_scores.append(metadata_score / total_metadata_checks)
                
                # Quality checks
                if len(reviews) == 0:
                    review_analysis["quality_issues"].append(f"Property {i+1}: Empty reviews array")
                elif len(reviews) < reviews_count * 0.8:  # Less than 80% of expected reviews
                    review_analysis["quality_issues"].append(f"Property {i+1}: Review count mismatch")
        
        if review_counts:
            review_analysis["review_statistics"] = {
                "avg_reviews_per_property": sum(review_counts) / len(review_counts),
                "max_reviews_extracted": max(review_counts),
                "min_reviews_extracted": min(review_counts),
                "total_reviews_extracted": sum(review_counts)
            }
        
        if metadata_scores:
            review_analysis["metadata_quality"] = {
                "average_metadata_completeness": sum(metadata_scores) / len(metadata_scores),
                "metadata_quality_grade": self._get_quality_grade(sum(metadata_scores) / len(metadata_scores))
            }
        
        review_analysis["review_extraction_rate"] = review_analysis["properties_with_reviews"] / len(properties)
        review_analysis["review_quality_grade"] = self._get_quality_grade(review_analysis["review_extraction_rate"])
        
        return review_analysis
    
    def validate_property_details(self, properties: List[Dict]) -> Dict[str, Any]:
        """Validate property detail accuracy and consistency."""
        detail_analysis = {
            "total_properties": len(properties),
            "detail_completeness": {},
            "accuracy_checks": {},
            "consistency_issues": []
        }
        
        # Analyze key property details
        details_to_check = ["bedrooms", "bathrooms", "beds", "host_name", "property_type"]
        
        for detail in details_to_check:
            populated_count = sum(1 for prop in properties if self._is_field_populated(prop, detail))
            completion_rate = populated_count / len(properties)
            
            detail_analysis["detail_completeness"][detail] = {
                "populated": populated_count,
                "completion_rate": completion_rate,
                "quality_grade": self._get_quality_grade(completion_rate)
            }
        
        # Accuracy checks
        for i, prop in enumerate(properties):
            # Bedroom/bathroom consistency
            bedrooms = prop.get("bedrooms", 0)
            bathrooms = prop.get("bathrooms", 0)
            
            if isinstance(bedrooms, (int, float)) and isinstance(bathrooms, (int, float)):
                if bedrooms > 0 and bathrooms == 0:
                    detail_analysis["consistency_issues"].append(f"Property {i+1}: Has bedrooms but no bathrooms")
                elif bedrooms > 10:
                    detail_analysis["consistency_issues"].append(f"Property {i+1}: Unusually high bedroom count ({bedrooms})")
            
            # URL validation
            url = prop.get("airbnb_url", "")
            if url and not url.startswith("https://www.airbnb.com"):
                detail_analysis["consistency_issues"].append(f"Property {i+1}: Invalid Airbnb URL format")
        
        # Overall detail quality
        completion_rates = [data["completion_rate"] for data in detail_analysis["detail_completeness"].values()]
        overall_detail_quality = sum(completion_rates) / len(completion_rates) if completion_rates else 0
        
        detail_analysis["overall_detail_quality"] = overall_detail_quality
        detail_analysis["detail_quality_grade"] = self._get_quality_grade(overall_detail_quality)
        
        return detail_analysis
    
    def calculate_comprehensive_quality_score(self, properties: List[Dict], level: int) -> Dict[str, Any]:
        """Calculate comprehensive quality score for the dataset."""
        completeness_analysis = self.analyze_field_completeness(properties, level)
        price_analysis = self.validate_price_accuracy(properties)
        detail_analysis = self.validate_property_details(properties)
        
        quality_components = {
            "data_completeness": completeness_analysis.get("overall_completeness", 0),
            "price_quality": price_analysis.get("price_completion_rate", 0),
            "detail_quality": detail_analysis.get("overall_detail_quality", 0)
        }
        
        # Add review quality for levels 3+
        if level >= 3:
            review_analysis = self.validate_review_quality(properties)
            quality_components["review_quality"] = review_analysis.get("review_extraction_rate", 0)
        
        # Calculate weighted comprehensive score
        weights = {
            "data_completeness": 0.4,
            "price_quality": 0.3,
            "detail_quality": 0.2,
            "review_quality": 0.1
        }
        
        comprehensive_score = 0
        for component, score in quality_components.items():
            weight = weights.get(component, 0)
            comprehensive_score += score * weight
        
        return {
            "comprehensive_quality_score": comprehensive_score,
            "quality_grade": self._get_quality_grade(comprehensive_score),
            "component_scores": quality_components,
            "component_grades": {comp: self._get_quality_grade(score) for comp, score in quality_components.items()},
            "quality_breakdown": {
                "completeness": completeness_analysis,
                "prices": price_analysis,
                "details": detail_analysis,
                "reviews": self.validate_review_quality(properties) if level >= 3 else None
            }
        }

def main():
    """Execute Phase 4.2 data quality validation."""
    print("🧪 PHASE 4.2: DATA QUALITY VALIDATION & METRICS")
    print("=" * 80)
    print("Testing data quality using existing successful job results...")
    
    validator = DataQualityValidator()
    
    # We'll use the successful tests from Phase 4.1 for quality analysis
    print("Data quality validation framework ready!")
    print("Use curl commands to analyze specific job results.")

if __name__ == "__main__":
    main()