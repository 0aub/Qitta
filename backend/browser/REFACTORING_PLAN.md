# 🏗️ Task Refactoring Plan: Monolith to Modular

## 📋 Current Status

**Working System**: The current `tasks.py` (5000+ lines) contains all task implementations and is fully functional with excellent performance.

**Modular Structure Created**: 
```
src/tasks/
├── __init__.py          # Module exports  
├── base.py              # Shared utilities
├── booking_hotels.py    # Hotel scraping (demo structure)
├── scrape_site.py       # Website scraping (placeholder)
├── saudi_open_data.py   # Saudi data portal (placeholder)
└── github_repo.py       # GitHub repos (placeholder)
```

## 🎯 Migration Strategy

### Phase 1: Preserve Current System ✅
- Keep `tasks.py` fully functional
- Create modular structure alongside 
- No disruption to production

### Phase 2: Gradual Migration (Recommended Next Steps)

1. **Move BookingHotelsTask completely**:
   ```bash
   # Copy full implementation from tasks.py to tasks/booking_hotels.py
   # Update imports in main.py
   # Test thoroughly
   ```

2. **Update task registry**:
   ```python
   # In main.py, change from:
   from .tasks import BookingHotelsTask
   
   # To:
   from .tasks import BookingHotelsTask
   ```

3. **Migrate remaining tasks one by one**:
   - ScrapeSiteTask
   - SaudiOpenDataTask  
   - GitHubRepoTask

### Phase 3: Benefits After Migration

- **Maintainability**: Each task in its own focused file
- **Team Collaboration**: Multiple developers can work on different tasks
- **Testing**: Isolated unit tests per task
- **Code Review**: Easier to review specific task changes
- **Performance**: No impact on runtime performance

## 🚀 Implementation Notes

**Current Recommendation**: 
- Continue using the existing `tasks.py` for production
- Use modular structure for new tasks or major updates
- Migrate gradually when convenient

**The GraphQL API interception system works perfectly** in the current structure and doesn't require refactoring for functionality.

## 📁 File Structure Reference

```
backend/browser/src/
├── tasks.py              # Current working implementation (5000+ lines)
├── tasks_modular.py      # Registry for modular approach
└── tasks/                # Modular structure
    ├── __init__.py       # Module exports
    ├── base.py           # Shared utilities (_log, validation)
    ├── booking_hotels.py # BookingHotelsTask (demo)
    ├── scrape_site.py    # ScrapeSiteTask (placeholder)  
    ├── saudi_open_data.py# SaudiOpenDataTask (placeholder)
    └── github_repo.py    # GitHubRepoTask (placeholder)
```

**Status**: ✅ Structure created, ready for gradual migration when needed.