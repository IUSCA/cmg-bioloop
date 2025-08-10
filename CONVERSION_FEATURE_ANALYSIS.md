# CMG vs CFNDAP Conversion Feature Analysis - Summary for AI Agent

## **Project Context**
- **Current Project**: `/Users/ripandey/dev/cmg-bioloop` (CFNDAP-based app)
- **Reference Project**: `/Users/ripandey/dev/cmg` (CMG legacy app)
- **Goal**: Port CMG's conversion functionality to the current CFNDAP-based app without losing any features

## **CMG Conversion Feature (Source)**

### **Architecture**
- **Simple MongoDB-based system** with basic conversion tracking
- **Direct pipeline processing** using string-based pipeline definitions
- **Basic status flow**: `new` → `working` → `completed`

### **Components**
- **API Routes**: `/api/conversions` with CRUD operations
- **Data Model**: Simple schema with user, dataset, pipeline, options, status fields
- **UI**: Vue.js components (Conversions.vue, ConversionDetails.vue, ConversionForm.vue)
- **Workers**: Python scripts for conversion processing (`conversions.py`, `rerun_conversions.py`)
- **Configuration**: Basic pipeline definitions in settings files

### **Key Features**
- Dataset selection and conversion initiation
- Pipeline configuration with options
- Status monitoring and progress tracking
- Conversion history and result viewing
- Basic error handling

## **CFNDAP Conversion Feature (Target)**

### **Architecture**
- **Sophisticated conversion registry system** with plugin-based architecture
- **Generic conversion framework** that can handle multiple conversion types
- **Advanced workflow integration** with other systems

### **Components**
- **API Routes**: `/api/conversions` with conversion definitions and management
- **Data Model**: Prisma/PostgreSQL schema with conversion definitions and instances
- **UI**: Modern UI components integrated with the current app's design system
- **Workers**: `convert.py` (generic conversion engine) and `source2raw.py` (specialized DICOM→BIDS)
- **Configuration**: Conversion definitions with type-specific parameters

### **Key Features**
- Conversion type registry and definitions
- Generic conversion processing framework
- Advanced error handling and status management
- Workflow integration capabilities
- Extensible architecture for new conversion types

## **Key Differences**

### **Complexity Level**
- **CMG**: Simple, direct approach focused on specific use cases
- **CFNDAP**: Sophisticated, extensible framework for any conversion type

### **Architecture**
- **CMG**: Monolithic conversion system
- **CFNDAP**: Plugin-based conversion registry

### **Data Management**
- **CMG**: Basic MongoDB schema
- **CFNDAP**: Structured Prisma/PostgreSQL with conversion definitions

## **Migration Strategy**

### **Recommended Approach**
1. **Reuse CFNDAP's `convert.py`** as the base conversion engine
2. **Extend the conversion registry** to include CMG's pipeline types
3. **Use current app's UI/UX** (no need to port CMG's UI components)
4. **Map CMG's pipeline configurations** to CFNDAP's definition system

### **What to Keep from CMG**
- Pipeline configurations and workflows
- Conversion parameters and options
- User experience patterns and requirements

### **What to Replace with CFNDAP**
- Backend conversion logic
- Data management and status tracking
- Error handling and monitoring

### **What to Adapt**
- Import paths and data model integration
- File path handling for current app's structure
- API response format alignment

## **Implementation Benefits**
- **Feature Parity**: All CMG conversion features can be preserved
- **Enhanced Capabilities**: Gain CFNDAP's advanced features and extensibility
- **Future-Proof**: More maintainable and extensible architecture
- **Minimal UI Changes**: Leverage existing CFNDAP UI components

## **Technical Notes**
- Both systems solve the same fundamental problem: data format conversion with user control and tracking
- CMG's approach is simpler but limited; CFNDAP's approach is more complex but powerful
- The migration represents an upgrade from a specialized tool to a flexible toolbox system
- Integration requires minimal changes to `convert.py` since it's designed to be framework-agnostic

## **Next Steps for AI Agent**
1. Examine the current app's conversion-related code structure
2. Identify where to integrate CMG's pipeline configurations
3. Adapt CFNDAP's `convert.py` for the current app's data models
4. Create conversion definitions that map to CMG's existing workflows
5. Test end-to-end conversion functionality

## **File Structure Reference**

### **CMG Project Files**
- `../cmg/api/routes/conversions.js` - API endpoints
- `../cmg/api/models/conversion.js` - Data model
- `../cmg/ui/src/views/Conversions.vue` - Main conversion view
- `../cmg/ui/src/views/ConversionDetails.vue` - Conversion details view
- `../cmg/ui/src/components/ConversionForm.vue` - Conversion form
- `../cmg/workers/cmg/worker/conversions.py` - Main conversion worker
- `../cmg/workers/rerun_conversions.py` - Conversion rerun utility
- `../cmg/workers/copy_conversion_reports.py` - Report copying utility

### **CFNDAP Project Files**
- `../cfndap/api/src/routes/conversions/index.js` - API endpoints
- `../cfndap/api/src/routes/conversions/definitions.js` - Conversion definitions
- `../cfndap/api/src/services/conversion.js` - Conversion service
- `../cfndap/api/prisma/schema.prisma` - Database schema
- `../cfndap/workers/workers/tasks/convert.py` - Generic conversion engine
- `../cfndap/workers/workers/tasks/source2raw.py` - Specialized DICOM converter
- `../cfndap/workers/workers/conversions_app.py` - Conversion application

## **Conversion Workflow Comparison**

### **CMG Workflow**
1. User selects dataset
2. User chooses pipeline from predefined options
3. User configures pipeline parameters
4. System processes conversion using simple pipeline strings
5. User monitors basic status updates
6. Results stored in simple output structure

### **CFNDAP Workflow**
1. User selects dataset
2. User chooses conversion type from registry
3. System presents type-specific configuration options
4. Conversion processed through generic engine with type-specific handlers
5. Advanced status tracking with detailed progress information
6. Results integrated with broader workflow system

## **Data Model Mapping**

### **CMG Schema → CFNDAP Schema**
- `user` → `userId` (foreign key relationship)
- `dataset` → `datasetId` (foreign key relationship)
- `pipeline` → `conversionTypeId` (references conversion definition)
- `options` → `parameters` (JSON field with conversion-specific options)
- `status` → `status` (enum with more granular states)
- `output_path` → `outputPath` (structured path management)
- `worker` → `workerId` (worker tracking)
- `staged` → `stagingStatus` (staging workflow integration)

## **Implementation Priority**

### **Phase 1: Core Infrastructure**
- Port CFNDAP's `convert.py` to current app
- Create conversion definitions for CMG's pipeline types
- Set up basic conversion registry

### **Phase 2: Feature Migration**
- Implement CMG's pipeline configurations
- Create conversion type handlers
- Set up status tracking and monitoring

### **Phase 3: Integration & Testing**
- Connect with current app's UI components
- Test end-to-end conversion workflows
- Validate feature parity with CMG

### **Phase 4: Enhancement**
- Add advanced features from CFNDAP
- Implement workflow integration capabilities
- Add monitoring and analytics

## **Risk Assessment**

### **Low Risk**
- Core conversion logic (well-tested in CFNDAP)
- Data model migration (structured approach)
- UI integration (using existing components)

### **Medium Risk**
- Pipeline configuration mapping
- Status flow alignment
- Error handling consistency

### **High Risk**
- File path compatibility
- Worker environment differences
- Performance optimization

## **Success Criteria**
- All CMG conversion features successfully ported
- No loss of functionality during migration
- Improved performance and reliability
- Enhanced extensibility for future conversion types
- Seamless user experience with current app's UI/UX 
