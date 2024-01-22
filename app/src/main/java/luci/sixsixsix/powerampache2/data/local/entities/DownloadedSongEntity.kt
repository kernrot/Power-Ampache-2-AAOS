package luci.sixsixsix.powerampache2.data.local.entities

import androidx.room.Entity
import androidx.room.PrimaryKey
import luci.sixsixsix.powerampache2.common.Constants
import luci.sixsixsix.powerampache2.domain.models.MusicAttribute
import luci.sixsixsix.powerampache2.domain.models.Song

@Entity
data class DownloadedSongEntity(
    @PrimaryKey val mediaId: String,
    val title: String,
    val albumId: String,
    val albumName: String,
    val artistId: String,
    val artistName: String,
    val songUri: String,
    val bitrate: Int,
    val channels: Int,
    val genre: List<MusicAttribute> = listOf(),
    val mime: String? = null,
    val name: String,
    val mode: String? = null,
    val streamFormat: String? = null,
    val format: String? = null,
    val disk: Int,
    val composer: String = "",
    val rateHz: Int = Constants.ERROR_INT,
    val size: Int = Constants.ERROR_INT,
    val time: Int = Constants.ERROR_INT,
    val trackNumber: Int = Constants.ERROR_INT,
    val year: Int = Constants.ERROR_INT,
    val imageUrl: String = "" ,
    val albumArtist: MusicAttribute = MusicAttribute.emptyInstance(),
    val averageRating: Float,
    val preciseRating: Float,
    val rating: Float,
    val lyrics: String = "",
    val comment: String = "",
    val language: String = "",
    val relativePath: String
)

fun Song.toDownloadedSongEntity(localUri: String) = DownloadedSongEntity(
    mediaId = mediaId,
    title = title,
    artistId = artist.id,
    artistName = artist.name,
    albumId = album.id,
    albumName = album.name,
    songUri = localUri,
    bitrate = bitrate,
    channels = channels,
    genre = genre,
    mime = mime,
    name = name,
    format = format,
    disk = disk,
    composer = composer,
    rateHz = rateHz,
    size = size,
    time = time,
    trackNumber = trackNumber,
    year = year,
    imageUrl = imageUrl,
    albumArtist = albumArtist,
    averageRating = averageRating,
    preciseRating = preciseRating,
    rating = rating,
    relativePath = filename
)

fun DownloadedSongEntity.toSong() = Song(
    mediaId = mediaId,
    title = title ?: "",
    artist = MusicAttribute(id = artistId, name = artistName),
    album = MusicAttribute(id = albumId, name = albumName),
    albumArtist = albumArtist,
    songUrl = songUri ?: "",
    imageUrl = imageUrl ?: "",
    bitrate = bitrate ?: Constants.ERROR_INT,
    channels = channels ?: Constants.ERROR_INT,
    composer = composer ?: "",
    genre = genre,
    mime = mime,
    name = name ?: "",
    rateHz = rateHz ?: Constants.ERROR_INT,
    size = size ?: Constants.ERROR_INT,
    time = time ?: Constants.ERROR_INT,
    trackNumber = trackNumber ?: Constants.ERROR_INT,
    year = year ?: Constants.ERROR_INT,
    mode = mode,
    streamFormat = streamFormat,
    format = format,
    lyrics = lyrics,
    comment = comment,
    language = language,
    disk = disk,
    preciseRating = preciseRating,
    averageRating = averageRating,
    rating = rating,
    filename = relativePath
)
