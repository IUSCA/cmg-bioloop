# Database & Prisma Patterns

## Prisma Schema Conventions

**Model naming:**
- Use `snake_case` for model names (matches PostgreSQL convention)
- Use `snake_case` for field names
- Use descriptive relation names

```prisma
model genome_browser_session {
  id                   Int             @id @default(autoincrement())
  title                String?
  user_id              Int?
  user                 user?           @relation(fields: [user_id], references: [id])
  session_tracks       session_track[]
}

model session_track {
  id         Int                    @id @default(autoincrement())
  session_id Int
  track_id   Int
  session    genome_browser_session @relation(fields: [session_id], references: [id])
  track      track                  @relation(fields: [track_id], references: [id])
  
  @@unique([session_id, track_id])
}
```

---

## Cascade Delete Patterns

Use `onDelete: Cascade` for dependent data:

```prisma
model track {
  id              Int          @id @default(autoincrement())
  dataset_file_id Int          @unique
  dataset_file    dataset_file @relation(fields: [dataset_file_id], references: [id], onDelete: Cascade)
}
```

---

## JSON Field Usage

Use `Json` type for flexible metadata:

```prisma
model dataset {
  metadata Json? // { stage_alias: "path/to/staged", custom_key: "value" }
}
```

Access in code:
```javascript
const stageAlias = dataset.metadata?.stage_alias || '';
```

---

## Shared Prisma Includes as Constants

**DO THIS:**
```javascript
// api/src/constants/prismaIncludes.js
export const INCLUDES = {
  SESSION_WITH_TRACKS: {
    session_tracks: {
      include: {
        track: {
          include: {
            dataset_file: {
              include: {
                dataset: {
                  include: { genomic_details: true },
                },
              },
            },
          },
        },
      },
    },
  },
  DATASET_WITH_FILES: {
    files: true,
    genomic_details: true,
  },
};

// In route file
const session = await prisma.genome_browser_session.findUnique({
  where: { id: sessionId },
  include: INCLUDES.SESSION_WITH_TRACKS,
});
```

**NOT THIS:**
```javascript
// Repeating the same include in multiple route files
const session = await prisma.genome_browser_session.findUnique({
  where: { id: sessionId },
  include: {
    session_tracks: {
      include: {
        track: {
          include: { /* ... deeply nested ... */ }
        }
      }
    }
  }
});
```

---

**Last Updated:** 2026-01-16

