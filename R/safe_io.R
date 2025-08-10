#' Safe file operations with automatic backup and atomic writes
#' 
#' This module provides robust file I/O operations that prevent data loss
#' through atomic writes, automatic backups, and comprehensive error handling.

# Configuration
.io_config <- list(
  backup_dir = "backups",
  max_backups = 10,
  validate_after_write = TRUE,
  use_atomic_writes = TRUE
)

#' Read file safely with multiple fallback options
#' @param path Primary file path
#' @param fallback_paths Alternative paths to try if primary fails
#' @param reader_fn Function to read the file (default: readLines)
#' @return File content or NULL on failure
safe_read_file <- function(path, fallback_paths = NULL, reader_fn = readLines) {
  
  # Try primary path
  if (file.exists(path)) {
    content <- tryCatch({
      reader_fn(path)
    }, error = function(e) {
      warning(sprintf("Failed to read %s: %s", path, e$message))
      NULL
    })
    
    if (!is.null(content)) return(content)
  }
  
  # Try fallbacks
  if (!is.null(fallback_paths)) {
    for (fallback in fallback_paths) {
      if (file.exists(fallback)) {
        content <- tryCatch({
          message(sprintf("Using fallback file: %s", fallback))
          reader_fn(fallback)
        }, error = function(e) {
          warning(sprintf("Fallback %s also failed: %s", fallback, e$message))
          NULL
        })
        
        if (!is.null(content)) return(content)
      }
    }
  }
  
  # Check for backups
  backup_pattern <- sprintf("%s\\.backup_.*", basename(path))
  backup_dir <- file.path(dirname(path), .io_config$backup_dir)
  
  if (dir.exists(backup_dir)) {
    backups <- list.files(backup_dir, pattern = backup_pattern, full.names = TRUE)
    
    if (length(backups) > 0) {
      # Sort by modification time, newest first
      backups <- backups[order(file.info(backups)$mtime, decreasing = TRUE)]
      
      for (backup in backups[1:min(3, length(backups))]) {  # Try 3 most recent
        content <- tryCatch({
          message(sprintf("Attempting recovery from backup: %s", backup))
          reader_fn(backup)
        }, error = function(e) {
          NULL
        })
        
        if (!is.null(content)) {
          warning(sprintf("Recovered data from backup: %s", backup))
          return(content)
        }
      }
    }
  }
  
  warning(sprintf("All read attempts failed for %s", path))
  return(NULL)
}

#' Write file safely with atomic operations and backup
#' @param content Content to write
#' @param path Target file path
#' @param writer_fn Function to write the file
#' @param create_backup Whether to backup existing file
#' @param validate_fn Function to validate written content
safe_write_file <- function(content, path, writer_fn = writeLines,
                          create_backup = TRUE, validate_fn = NULL) {
  
  # Ensure directory exists
  dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
  
  # Check write permissions
  if (file.exists(path) && file.access(path, 2) != 0) {
    stop(sprintf("No write permission for %s", path))
  }
  
  if (!file.exists(dirname(path)) || file.access(dirname(path), 2) != 0) {
    stop(sprintf("No write permission for directory %s", dirname(path)))
  }
  
  # Create backup if file exists
  backup_path <- NULL
  if (create_backup && file.exists(path)) {
    backup_path <- create_backup_file(path)
    if (is.null(backup_path)) {
      warning("Failed to create backup, proceeding with caution")
    }
  }
  
  # Atomic write: write to temp file first
  temp_file <- tempfile(tmpdir = dirname(path), 
                        pattern = paste0(".", basename(path), ".tmp"))
  
  write_success <- FALSE
  
  tryCatch({
    # Write to temp file
    if (is.function(writer_fn)) {
      writer_fn(content, temp_file)
    } else {
      stop("writer_fn must be a function")
    }
    
    # Validate if validator provided
    if (!is.null(validate_fn)) {
      validation_result <- tryCatch({
        validate_fn(temp_file)
      }, error = function(e) {
        list(valid = FALSE, error = e$message)
      })
      
      if (is.logical(validation_result)) {
        validation_result <- list(valid = validation_result)
      }
      
      if (!validation_result$valid) {
        stop(sprintf("Validation failed: %s", 
                    validation_result$error %||% "Unknown error"))
      }
    }
    
    # Atomic rename (on same filesystem, this is atomic)
    if (.io_config$use_atomic_writes) {
      # On Windows, need to remove target first
      if (Sys.info()["sysname"] == "Windows" && file.exists(path)) {
        file.remove(path)
      }
      file.rename(temp_file, path)
    } else {
      file.copy(temp_file, path, overwrite = TRUE)
      file.remove(temp_file)
    }
    
    write_success <- TRUE
    
    # Verify write
    if (.io_config$validate_after_write && file.exists(path)) {
      verify_content <- tryCatch({
        reader_fn <- switch(
          class(writer_fn)[1],
          "standardGeneric" = get(paste0("read", gsub("write", "", deparse(substitute(writer_fn))))),
          readLines
        )
        reader_fn(path)
      }, error = function(e) NULL)
      
      if (is.null(verify_content)) {
        warning("Could not verify written content")
      }
    }
    
    # Clean up old backups
    if (write_success && !is.null(backup_path)) {
      cleanup_old_backups(dirname(backup_path), basename(path))
    }
    
    message(sprintf("Successfully wrote %s", path))
    return(TRUE)
    
  }, error = function(e) {
    # Clean up temp file
    if (file.exists(temp_file)) {
      file.remove(temp_file)
    }
    
    # Attempt restore from backup
    if (!is.null(backup_path) && file.exists(backup_path)) {
      restore_success <- tryCatch({
        file.copy(backup_path, path, overwrite = TRUE)
        TRUE
      }, error = function(e) FALSE)
      
      if (restore_success) {
        warning(sprintf("Write failed, restored from backup: %s", e$message))
      } else {
        stop(sprintf("Write failed and restore failed: %s", e$message))
      }
    } else {
      stop(sprintf("Write failed: %s", e$message))
    }
    
    return(FALSE)
  })
}

#' Create backup of file
create_backup_file <- function(path) {
  if (!file.exists(path)) return(NULL)
  
  backup_dir <- file.path(dirname(path), .io_config$backup_dir)
  dir.create(backup_dir, showWarnings = FALSE, recursive = TRUE)
  
  timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  backup_name <- sprintf("%s.backup_%s", basename(path), timestamp)
  backup_path <- file.path(backup_dir, backup_name)
  
  tryCatch({
    file.copy(path, backup_path, overwrite = FALSE)
    message(sprintf("Created backup: %s", backup_path))
    backup_path
  }, error = function(e) {
    warning(sprintf("Failed to create backup: %s", e$message))
    NULL
  })
}

#' Clean up old backup files
cleanup_old_backups <- function(backup_dir, original_filename) {
  if (!dir.exists(backup_dir)) return()
  
  pattern <- sprintf("%s\\.backup_.*", original_filename)
  backups <- list.files(backup_dir, pattern = pattern, full.names = TRUE)
  
  if (length(backups) > .io_config$max_backups) {
    # Sort by modification time
    backups <- backups[order(file.info(backups)$mtime, decreasing = FALSE)]
    
    # Remove oldest
    to_remove <- backups[1:(length(backups) - .io_config$max_backups)]
    
    for (backup in to_remove) {
      tryCatch({
        file.remove(backup)
        message(sprintf("Removed old backup: %s", basename(backup)))
      }, error = function(e) {
        warning(sprintf("Could not remove old backup %s: %s", backup, e$message))
      })
    }
  }
}

#' Safe YAML operations
safe_read_yaml <- function(path, fallback_paths = NULL) {
  safe_read_file(
    path = path,
    fallback_paths = fallback_paths,
    reader_fn = yaml::read_yaml
  )
}

safe_write_yaml <- function(data, path, create_backup = TRUE) {
  safe_write_file(
    content = data,
    path = path,
    writer_fn = yaml::write_yaml,
    create_backup = create_backup,
    validate_fn = function(temp_path) {
      # Validate YAML structure
      test_read <- tryCatch({
        yaml::read_yaml(temp_path)
      }, error = function(e) NULL)
      
      if (is.null(test_read)) {
        return(list(valid = FALSE, error = "Invalid YAML structure"))
      }
      
      # Check if same structure
      if (!identical(names(test_read), names(data))) {
        return(list(valid = FALSE, 
                   error = "YAML structure mismatch after write"))
      }
      
      return(list(valid = TRUE))
    }
  )
}

#' Safe CSV operations
safe_read_csv <- function(path, fallback_paths = NULL, ...) {
  safe_read_file(
    path = path,
    fallback_paths = fallback_paths,
    reader_fn = function(p) readr::read_csv(p, show_col_types = FALSE, ...)
  )
}

safe_write_csv <- function(data, path, create_backup = TRUE, append = FALSE) {
  if (append && file.exists(path)) {
    # For append, we need special handling
    existing <- safe_read_csv(path)
    if (!is.null(existing)) {
      data <- dplyr::bind_rows(existing, data)
    }
  }
  
  safe_write_file(
    content = data,
    path = path,
    writer_fn = function(d, p) readr::write_csv(d, p),
    create_backup = create_backup,
    validate_fn = function(temp_path) {
      test_read <- tryCatch({
        readr::read_csv(temp_path, show_col_types = FALSE)
      }, error = function(e) NULL)
      
      if (is.null(test_read)) {
        return(list(valid = FALSE, error = "Cannot read back CSV"))
      }
      
      if (nrow(test_read) != nrow(data)) {
        return(list(valid = FALSE, 
                   error = sprintf("Row count mismatch: expected %d, got %d",
                                 nrow(data), nrow(test_read))))
      }
      
      return(list(valid = TRUE))
    }
  )
}

#' Safe RDS operations
safe_read_rds <- function(path, fallback_paths = NULL) {
  safe_read_file(
    path = path,
    fallback_paths = fallback_paths,
    reader_fn = readRDS
  )
}

safe_write_rds <- function(data, path, create_backup = TRUE, compress = TRUE) {
  safe_write_file(
    content = data,
    path = path,
    writer_fn = function(d, p) saveRDS(d, p, compress = compress),
    create_backup = create_backup,
    validate_fn = function(temp_path) {
      test_read <- tryCatch({
        readRDS(temp_path)
      }, error = function(e) NULL)
      
      if (is.null(test_read)) {
        return(list(valid = FALSE, error = "Cannot read back RDS file"))
      }
      
      return(list(valid = TRUE))
    }
  )
}

#' Configure safe I/O settings
#' @param backup_dir Directory for backups
#' @param max_backups Maximum number of backups to keep
#' @param validate_after_write Whether to validate after writing
#' @param use_atomic_writes Whether to use atomic write operations
configure_safe_io <- function(backup_dir = NULL, max_backups = NULL,
                             validate_after_write = NULL, use_atomic_writes = NULL) {
  if (!is.null(backup_dir)) {
    .io_config$backup_dir <<- backup_dir
  }
  if (!is.null(max_backups)) {
    .io_config$max_backups <<- max_backups
  }
  if (!is.null(validate_after_write)) {
    .io_config$validate_after_write <<- validate_after_write
  }
  if (!is.null(use_atomic_writes)) {
    .io_config$use_atomic_writes <<- use_atomic_writes
  }
  
  invisible(.io_config)
}

#' Get current safe I/O configuration
get_safe_io_config <- function() {
  .io_config
}

#' Define null coalescing operator if not already defined
if (!exists("%||%")) {
  `%||%` <- function(x, y) {
    if (is.null(x)) y else x
  }
}