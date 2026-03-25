package com.pulsaradmin.worker;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class WorkerReplayCopyJobCriteriaRepository {
  private final JdbcTemplate jdbcTemplate;

  public WorkerReplayCopyJobCriteriaRepository(JdbcTemplate jdbcTemplate) {
    this.jdbcTemplate = jdbcTemplate;
  }

  public List<Map<String, String>> findRowsByJobId(String jobId) {
    List<CriteriaCell> cells = jdbcTemplate.query("""
        select row_index, field_name, field_value
        from replay_copy_job_criteria
        where job_id = ?
        order by row_index, id
        """, (rs, rowNum) -> new CriteriaCell(
        rs.getInt("row_index"),
        rs.getString("field_name"),
        rs.getString("field_value")), jobId);

    List<Map<String, String>> rows = new ArrayList<>();
    int currentIndex = -1;
    Map<String, String> currentRow = null;
    for (CriteriaCell cell : cells) {
      if (cell.rowIndex() != currentIndex) {
        currentRow = new LinkedHashMap<>();
        rows.add(currentRow);
        currentIndex = cell.rowIndex();
      }
      if (currentRow != null && cell.fieldName() != null && cell.fieldValue() != null) {
        currentRow.put(cell.fieldName(), cell.fieldValue());
      }
    }
    return rows;
  }

  private record CriteriaCell(int rowIndex, String fieldName, String fieldValue) {
  }
}
