package luci.sixsixsix.powerampache2.data.remote.worker

import android.content.Context
import androidx.hilt.work.HiltWorker
import androidx.work.BackoffPolicy
import androidx.work.Constraints
import androidx.work.CoroutineWorker
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.WorkManager
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.google.gson.Gson
import dagger.assisted.Assisted
import dagger.assisted.AssistedInject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import luci.sixsixsix.mrlog.L
import luci.sixsixsix.powerampache2.data.local.MusicDatabase
import luci.sixsixsix.powerampache2.data.local.entities.toDownloadedSongEntity
import luci.sixsixsix.powerampache2.data.remote.MainNetwork
import luci.sixsixsix.powerampache2.domain.models.Song
import luci.sixsixsix.powerampache2.domain.utils.StorageManager
import okhttp3.internal.http.HTTP_OK
import java.time.Duration
import java.util.UUID

@HiltWorker
class SongDownloadWorker @AssistedInject constructor(
    private val api: MainNetwork,
    val db: MusicDatabase,
    private val storageManager: StorageManager,
    @Assisted val context: Context,
    @Assisted private val params: WorkerParameters,
) : CoroutineWorker(context, params) {

    override suspend fun doWork(): Result = withContext(Dispatchers.IO) {
        val authKey = params.inputData.getString(KEY_AUTH_TOKEN)
        val username = params.inputData.getString(KEY_USERNAME)
        val songStr = params.inputData.getString(KEY_SONG)
        val song = Gson().fromJson(songStr, Song::class.java)

        val firstUpdate = workDataOf(KEY_PROGRESS to 0, KEY_SONG to "${song.artist.name} - ${song.name}")
        val lastUpdate = workDataOf(KEY_PROGRESS to 100, KEY_SONG to "${song.artist.name} - ${song.name}")

        setProgress(firstUpdate)

        api.downloadSong(
            authKey = authKey!!,
            songId = song.mediaId
        ).run {
            if (code() == HTTP_OK) {
                // save file to disk and register in database
                body()?.byteStream()?.let { inputStream ->
                    val filepath = storageManager.saveSong(song, inputStream)
                    db.dao.addDownloadedSong( // TODO fix double-bang!!
                        song.toDownloadedSongEntity(filepath, username!!)
                    )
                    setProgress(lastUpdate)
                    //if ()
                    Result.success(
                        workDataOf(KEY_RESULT_SONG to songStr)
                    )
                } ?: Result.failure(
                    workDataOf(
                        KEY_RESULT_ERROR to "cannot download/save file, " +
                            "body or input stream NULL response code: ${code()}"))
            } else {
                Result.failure(
                    workDataOf(
                        KEY_RESULT_ERROR to "cannot download/save file, " +
                            "response code: ${code()}, response body: ${body().toString()}"))
            }
        }
    }

    companion object {
        private const val prefix = "luci.sixsixsix.powerampache2.worker."

        const val KEY_USERNAME = "${prefix}KEY_USERNAME"
        const val KEY_AUTH_TOKEN = "${prefix}KEY_AUTH_TOKEN"
        const val KEY_SONG = "${prefix}KEY_SONG"
        const val KEY_RESULT_PATH = "${prefix}KEY_RESULT_PATH"
        const val KEY_RESULT_SONG = "${prefix}KEY_RESULT_SONG"
        const val KEY_RESULT_ERROR = "${prefix}KEY_RESULT_ERROR"
        const val KEY_PROGRESS = "${prefix}KEY_PROGRESS"

        fun startSongDownloadWorker(
            context: Context,
            workerName: String,
            authToken: String,
            username: String,
            song: Song
        ): UUID {
            val request = OneTimeWorkRequestBuilder<SongDownloadWorker>()
                .setInputData(
                    workDataOf(
                        KEY_SONG to Gson().toJson(song),
                        KEY_AUTH_TOKEN to authToken,
                        KEY_USERNAME to username)
                ).setConstraints(
                    Constraints(
                        requiresStorageNotLow = true,
                        requiredNetworkType = NetworkType.CONNECTED)
                ).setBackoffCriteria(
                    backoffPolicy = BackoffPolicy.LINEAR,
                    Duration.ofSeconds(10L)
                ).build()
            WorkManager.getInstance(context)
                .enqueueUniqueWork(workerName, ExistingWorkPolicy.APPEND, request)
            return request.id
        }

        fun stopAllDownloads(context: Context, workerName: String) {
            WorkManager.getInstance(context).cancelUniqueWork(workerName)
            // change worker name otherwise cannot restart work
        }
    }
}
