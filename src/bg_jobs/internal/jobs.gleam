import birl
import birl/duration
import gleam/option

// Job
//---------------

/// It's the data representing a job in the queue.
///
/// It contains all the necessary information to process the job.
///
pub type Job {
  Job(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    reserved_at: option.Option(#(#(Int, Int, Int), #(Int, Int, Int))),
    reserved_by: option.Option(String),
  )
}

/// Enum representing 3 ways of setting a jobs available_at 
///
pub type JobAvailability {
  AvailableNow
  AvailableAt(#(#(Int, Int, Int), #(Int, Int, Int)))
  AvailableIn(Int)
}

/// Data that is needed to enqueue a new job
///
pub type JobEnqueueRequest {
  JobEnqueueRequest(
    name: String,
    payload: String,
    availability: JobAvailability,
  )
}

/// Create a new job_request
///
pub fn new_job(name: String, payload: String) {
  JobEnqueueRequest(name:, payload:, availability: AvailableNow)
}

/// Set the availability of the job_request to a specific date-time
///
pub fn job_with_available_at(
  job_request: JobEnqueueRequest,
  availabile_at: #(#(Int, Int, Int), #(Int, Int, Int)),
) {
  JobEnqueueRequest(..job_request, availability: AvailableAt(availabile_at))
}

/// Set the availability of the job_request to a time in the future
///
pub fn job_with_available_in(job_request: JobEnqueueRequest, availabile_in: Int) {
  JobEnqueueRequest(..job_request, availability: AvailableIn(availabile_in))
}

/// Convert JobAvailability to erlang date time
///
pub fn available_at_from_availability(availability: JobAvailability) {
  case availability {
    AvailableNow -> {
      birl.now() |> birl.to_erlang_datetime()
    }
    AvailableAt(date) -> {
      date
      |> birl.from_erlang_local_datetime()
      |> birl.to_erlang_datetime()
    }
    AvailableIn(delay) -> {
      birl.now()
      |> birl.add(duration.milli_seconds(delay))
      |> birl.to_erlang_datetime()
    }
  }
}

/// Holds data about the outcome of processed a job.
///
/// It can either be a successful job or a failed job.
pub type SucceededJob {
  SucceededJob(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    succeded_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}

pub type FailedJob {
  FailedJob(
    id: String,
    name: String,
    payload: String,
    attempts: Int,
    exception: String,
    created_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    available_at: #(#(Int, Int, Int), #(Int, Int, Int)),
    failed_at: #(#(Int, Int, Int), #(Int, Int, Int)),
  )
}
