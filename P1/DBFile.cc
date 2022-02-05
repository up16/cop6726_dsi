#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include <iostream>
// stub file .. replace it with your own DBFile.cc

DBFile::DBFile()
{
}

int DBFile::Create(const char *f_path, fType f_type, void *startup)
{
  switch (f_type)
  {
  case heap:
    // Initialize heap buffer
    bufferFile.Open(0, const_cast<char *>(f_path));
    pageIndex = 0;
    writeFlag = false;
    MoveFirst();
    break;

  default:
    break;
  }
  // db file created successfully
  return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath)
{
  FILE *tableFile = fopen(loadpath, "r");
  Record r;
  ComparisonEngine c;

  while (r.SuckNextRecord(&f_schema, tableFile))
    this->Add(r);
  if (writeFlag)
    bufferFile.AddPage(&bufferPage, pageIndex);
}

int DBFile::Open(const char *f_path)
{
  bufferFile.Open(1, const_cast<char *>(f_path));
  pageIndex = 0;
  writeFlag = false;
  MoveFirst();
  return 1;
}

void DBFile::MoveFirst()
{
  if (writeFlag)
  {
    bufferFile.AddPage(&bufferPage, pageIndex);
    writeFlag = false;
  }
  pageIndex = 0;
  bufferPage.EmptyItOut();
  if (bufferFile.GetLength())
    bufferFile.GetPage(&bufferPage, pageIndex);
}

int DBFile::Close()
{
  if (writeFlag)
    bufferFile.AddPage(&bufferPage, pageIndex);
  bufferFile.Close();
  cout << "Length of file: " << bufferFile.GetLength() << "Pages"
       << "\nClosing file...";

  return 1;
}

void DBFile::Add(Record &rec)
{
  if (!writeFlag)
  {
    bufferPage.EmptyItOut();
    if (bufferFile.GetLength() > 0)
    {
      bufferFile.GetPage(&bufferPage, bufferFile.GetLength() - 2);
      pageIndex = bufferFile.GetLength() - 2;
    }
    writeFlag = true;
  }
  if (bufferPage.Append(&rec) == 0)
  {
    bufferFile.AddPage(&bufferPage, pageIndex++);
    bufferPage.EmptyItOut();
    bufferPage.Append(&rec);
  }
}

int DBFile::GetNext(Record &fetchme)
{
  if (writeFlag)
    MoveFirst();
  if (bufferPage.GetFirst(&fetchme) == 0)
  {
    pageIndex++;

    if (pageIndex >= bufferFile.GetLength() - 1)
    {
      return 0;
    }
    bufferPage.EmptyItOut();
    bufferFile.GetPage(&bufferPage, pageIndex);
    bufferPage.GetFirst(&fetchme);
  }
  return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal)
{
  ComparisonEngine c;
  while (GetNext(fetchme) == 1)
  {
    if (c.Compare(&fetchme, &literal, &cnf) == 1)
      return 1;
  }
  return 0;
}
