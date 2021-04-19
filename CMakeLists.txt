
cmake_minimum_required(VERSION 3.0)
project(infraql)

set(TARGET infraql_build)
set(EXECUTABLE infraql)

set(GOLANG_SRC_PATH_STR infraql)
set(GOLANG_BUILD_PATH_STR build)

set(OS_STR_MAC darwin)
set(OS_STR_LINUX linux)
set(OS_STR_WINDOWS windows)

set(EXE_SUFFIX_MAC _${OS_STR_MAC})
set(EXE_SUFFIX_LINUX _${OS_STR_LINUX})
set(EXE_SUFFIX_WINDOWS .exe)

set(BUILDMAJORVERSION 0)
set(BUILDMINORVERSION 0)
set(BUILDPATCHVERSION 1)
set(BUILDCOMMITSHA local-dev)
set(BUILDSHORTCOMMITSHA local-dev)
set(BUILDDATE 1900-01-01)

enable_testing()

set(TEST_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/test/python/main.py)

add_custom_target(${TARGET} ALL)

add_custom_command(TARGET ${TARGET}
  PRE_BUILD
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND go test ./...
  COMMENT "Running Go Unit Tests"
)

add_custom_command(TARGET ${TARGET}
  PRE_BUILD
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND go build -ldflags
  "-X infraql/internal/iql/cmd.BuildMajorVersion=${BUILDMAJORVERSION} \
  -X infraql/internal/iql/cmd.BuildMinorVersion=${BUILDMINORVERSION} \
  -X infraql/internal/iql/cmd.BuildPatchVersion=${BUILDPATCHVERSION} \
  -X infraql/internal/iql/cmd.BuildCommitSHA=${BUILDCOMMITSHA} \
  -X infraql/internal/iql/cmd.BuildShortCommitSHA=${BUILDSHORTCOMMITSHA} \
  -X \"infraql/internal/iql/cmd.BuildDate=${BUILDDATE}\" \
  -X infraql/internal/iql/cmd.BuildPlatform=$ENV{GOOS}"
  -o "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_BUILD_PATH_STR}/${EXECUTABLE}"
  "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_SRC_PATH_STR}"
  COMMENT "Building Go executable for local platform: $ENV{GOOS}"
)

add_custom_command(TARGET ${TARGET}
  PRE_BUILD
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E env GOOS=${OS_STR_MAC} go build -ldflags
  "-X infraql/internal/iql/cmd.BuildMajorVersion=${BUILDMAJORVERSION} \
  -X infraql/internal/iql/cmd.BuildMinorVersion=${BUILDMINORVERSION} \
  -X infraql/internal/iql/cmd.BuildPatchVersion=${BUILDPATCHVERSION} \
  -X infraql/internal/iql/cmd.BuildCommitSHA=${BUILDCOMMITSHA} \
  -X infraql/internal/iql/cmd.BuildShortCommitSHA=${BUILDSHORTCOMMITSHA} \
  -X \"infraql/internal/iql/cmd.BuildDate=${BUILDDATE}\" \
  -X infraql/internal/iql/cmd.BuildPlatform=${OS_STR_MAC}"
  -o "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_BUILD_PATH_STR}/${EXECUTABLE}${EXE_SUFFIX_MAC}"
  "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_SRC_PATH_STR}"
  COMMENT "Building Go executable for ${OS_STR_MAC}"
)

add_custom_command(TARGET ${TARGET}
  PRE_BUILD
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E env GOOS=${OS_STR_WINDOWS} go build -ldflags
  "-X infraql/internal/iql/cmd.BuildMajorVersion=${BUILDMAJORVERSION} \
  -X infraql/internal/iql/cmd.BuildMinorVersion=${BUILDMINORVERSION} \
  -X infraql/internal/iql/cmd.BuildPatchVersion=${BUILDPATCHVERSION} \
  -X infraql/internal/iql/cmd.BuildCommitSHA=${BUILDCOMMITSHA} \
  -X infraql/internal/iql/cmd.BuildShortCommitSHA=${BUILDSHORTCOMMITSHA} \
  -X \"infraql/internal/iql/cmd.BuildDate=${BUILDDATE}\" \
  -X infraql/internal/iql/cmd.BuildPlatform=${OS_STR_WINDOWS}"
  -o "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_BUILD_PATH_STR}/${EXECUTABLE}${EXE_SUFFIX_WINDOWS}"
  "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_SRC_PATH_STR}"
  COMMENT "Building Go executable for ${OS_STR_WINDOWS}"
)

add_custom_command(TARGET ${TARGET}
  PRE_BUILD
  DEPENDS ${SRCS}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND ${CMAKE_COMMAND} -E env GOOS=${OS_STR_LINUX} go build -ldflags
  "-X infraql/internal/iql/cmd.BuildMajorVersion=${BUILDMAJORVERSION} \
  -X infraql/internal/iql/cmd.BuildMinorVersion=${BUILDMINORVERSION} \
  -X infraql/internal/iql/cmd.BuildPatchVersion=${BUILDPATCHVERSION} \
  -X infraql/internal/iql/cmd.BuildCommitSHA=${BUILDCOMMITSHA} \
  -X infraql/internal/iql/cmd.BuildShortCommitSHA=${BUILDSHORTCOMMITSHA} \
  -X \"infraql/internal/iql/cmd.BuildDate=${BUILDDATE}\" \
  -X infraql/internal/iql/cmd.BuildPlatform=${OS_STR_LINUX}"
  -o "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_BUILD_PATH_STR}/${EXECUTABLE}${EXE_SUFFIX_LINUX}"
  "${CMAKE_CURRENT_SOURCE_DIR}/${GOLANG_SRC_PATH_STR}"
  COMMENT "Building Go executable for ${OS_STR_LINUX}"
)

find_program(Python_EXECUTABLE python3)


if(DEFINED ENV{INFRAQL_TEST_LOG_LEVEL})
  set(INFRAQL_TEST_LOG_LEVEL $ENV{INFRAQL_TEST_LOG_LEVEL})
else()
  set(INFRAQL_TEST_LOG_LEVEL warn)
endif()

if(DEFINED LIVE_INTEGRATION_TESTS)
  message(STATUS "LIVE_INTEGRATION_TESTS cmake variable = '${LIVE_INTEGRATION_TESTS}'")
else()
  set(LIVE_INTEGRATION_TESTS "")
  message(STATUS "LIVE_INTEGRATION_TESTS cmake variable is empty, only 'always' tests will be run")
endif()

if(NOT Python_EXECUTABLE)
    message(FATAL_ERROR "Cannot find the python3 executable!")
endif()
add_test(NAME int-tests
    COMMAND ${Python_EXECUTABLE} ${TEST_SCRIPT} --additionalintegrationtestdir=${LIVE_INTEGRATION_TESTS} --loglevel=${INFRAQL_TEST_LOG_LEVEL}
)

add_custom_command(TARGET ${TARGET}
  POST_BUILD
  COMMAND ctest --verbose)


